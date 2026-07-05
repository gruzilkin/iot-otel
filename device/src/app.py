"""Raspberry Pi sensor client: reads SCD30, SGP40, LPS22 and MSA311 over I2C and
streams readings to the iotd gRPC ingest service.

Transport is gRPC client-streaming with device-side timestamps. Auth is a bearer
device token in gRPC metadata.

Env:
  TARGET              host:port of the ingest service (e.g. iot.example.com:50051)
  BEARER              device access token
  TLS                 "true" to use a secure channel (recommended in production)
  TEMPERATURE_OFFSET  optional SCD30 calibration
  ALTITUDE            optional SCD30 altitude (m)
"""
import asyncio
import os
import signal
import time

import adafruit_lps2x
import adafruit_scd30
import adafruit_sgp40
import grpc
from adafruit_extended_bus import ExtendedI2C
from adafruit_msa3xx import MSA311, BandWidth, DataRate, Mode, Range, Resolution
from google.protobuf.timestamp_pb2 import Timestamp

import ingest_pb2
import ingest_pb2_grpc

# Bounded so a server/network stall drops the oldest readings instead of growing
# memory without bound on the Pi.
QUEUE_MAX = 1000
RECONNECT_DELAY = 5
SHUTDOWN_DRAIN_TIMEOUT = 10  # seconds to flush the backlog on shutdown before giving up

# Enqueued once at shutdown so the streamer flushes the remaining backlog and then
# half-closes the gRPC stream cleanly, rather than being cancelled mid-send.
_SHUTDOWN = object()

# Open the Linux I2C device directly. Using board.SCL/board.SDA makes Blinka
# perform GPIO board detection, which is unreliable inside a container even when
# /dev/i2c-1 is correctly mapped. Linux controls the bus frequency; the frequency
# argument accepted by CircuitPython busio is not settable through this backend.
i2c = ExtendedI2C(1)
scd = adafruit_scd30.SCD30(i2c)
sgp = adafruit_sgp40.SGP40(i2c)
lps = adafruit_lps2x.LPS22(i2c)  # barometric pressure, default addr 0x5D
msa = MSA311(i2c)  # 3-axis accelerometer, fixed addr 0x62
# Tuned for sensitivity to faint vibration (someone walking by, tremors) rather than
# large motion. Range and bandwidth are the levers that actually move the noise floor:
#   range 2g: finest quantization (~0.24 mg/LSB vs 0.49 at 4g); these sources never hit 2g.
#   bandwidth 15.63 Hz: RMS noise scales with sqrt(bandwidth), so narrowing the analog
#     low-pass from the 250 Hz default drops the resting floor ~4x (~0.05 -> ~0.012 m/s2).
#     Footstep/quake energy is almost entirely < ~30 Hz, so nothing useful is filtered out.
#   resolution 14-bit: already the library default and the max; set explicitly to document
#     intent (the driver's scaling always uses the full 14 bits regardless of this setting).
msa.power_mode = Mode.NORMAL
msa.range = Range.RANGE_2_G
msa.resolution = Resolution.RESOLUTION_14_BIT
msa.data_rate = DataRate.RATE_62_5_HZ  # output rate; >= our steady 50 Hz reads, so each is fresh
msa.bandwidth = BandWidth.WIDTH_15_63_HZ

temperature, humidity = None, None


async def offer(queue, name, value, ts):
    if queue.full():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
    await queue.put((name, float(value), ts))


async def read_sgp40(queue):
    while True:
        if temperature is not None and humidity is not None:
            voc_index = sgp.measure_index(temperature=temperature, relative_humidity=humidity)
            if voc_index != 0:
                await offer(queue, "voc", voc_index, now_timestamp())
        await asyncio.sleep(1)


async def read_scd30(queue):
    while True:
        if scd.data_available:
            global temperature, humidity
            temperature = scd.temperature
            humidity = scd.relative_humidity
            ts = now_timestamp()
            await offer(queue, "temperature", temperature, ts)
            await offer(queue, "humidity", humidity, ts)
            await offer(queue, "ppm", scd.CO2, ts)
        await asyncio.sleep(2.1)


def now_timestamp():
    ts = Timestamp()
    ts.GetCurrentTime()  # device-side UTC timestamp of the measurement
    return ts


async def read_lps22(queue):
    while True:
        await offer(queue, "pressure", lps.pressure, now_timestamp())  # hPa
        await asyncio.sleep(1)


# Accelerometer sampling for the "vibration" signal. We sample at a single steady
# rate: with the analog bandwidth capped at 15.63 Hz (see the MSA311 setup above) there
# is no content above ~16 Hz to chase, so the old idle/active rate switch bought nothing.
# The signal is the norm of the per-axis deviation from a slowly-relearned resting
# vector, sqrt(dx^2+dy^2+dz^2) — 1st-order sensitive in every direction, unlike the
# scalar |accel|-baseline, which is nearly blind to lateral motion (footsteps).
# Emission stays adaptive to keep DB volume down: during/just after an event (within
# QUIET_S of the last over-threshold sample) we stream every sample; when quiet we emit
# only the per-second peak (~0 at rest).
SAMPLE_HZ = 50  # one steady rate; > 2x the 15.63 Hz bandwidth, catches onset within ~20 ms
QUIET_S = 1.0  # keep streaming this long after the last over-threshold sample
# Adaptive trigger. Rather than a fixed cutoff, we track the running mean and standard
# deviation of the quiet-state deviation norm and fire at mean + K_SIGMA * std, so the
# device self-calibrates to its own noise floor and mounting. K_SIGMA is the sensitivity
# vs false-positive knob: Chebyshev caps the quiet-state exceedance at 1/K_SIGMA^2 for
# ANY distribution (~6% at 4, ~4% at 5); for near-Gaussian noise it is far lower. Raise
# it if idle still trips events, lower it to catch fainter movement.
K_SIGMA = 4.0
STATS_TAU_S = 10.0  # time constant of the running noise mean/variance (steadier than baseline)
STATS_ALPHA = (1 / SAMPLE_HZ) / STATS_TAU_S
# Relearn the resting orientation as a time constant, not a per-sample weight, so the
# drift rate is independent of SAMPLE_HZ (the EMA runs once per quiet sample): alpha = dt / tau.
BASELINE_TAU_S = 5.0
BASELINE_ALPHA = (1 / SAMPLE_HZ) / BASELINE_TAU_S


async def read_msa311(queue):
    bx, by, bz = msa.acceleration  # per-axis resting baseline (the ~9.81 gravity vector)
    # Running noise statistics of the quiet-state deviation norm: EWMA of the value and of
    # its square give a streaming mean and variance. Seeded high (std ~0.1) so the initial
    # threshold sits well above any real noise and converges DOWN over the first few
    # STATS_TAU as quiet samples arrive — it never trips on the way down, so there is no
    # warmup special-case. (Seeding low is unsafe: real noise would read as an event, the
    # stats would freeze, and the threshold could never rise.)
    mean = 0.0
    mean_sq = 0.1**2
    last_move = 0.0
    win_start = time.monotonic()
    win_max = 0.0

    while True:
        now = time.monotonic()
        x, y, z = msa.acceleration
        dx, dy, dz = x - bx, y - by, z - bz
        dyn = (dx * dx + dy * dy + dz * dz) ** 0.5  # ~0 at rest, spikes on movement

        std = max(mean_sq - mean * mean, 0.0) ** 0.5
        threshold = mean + K_SIGMA * std

        if dyn > threshold:
            last_move = now
        else:
            # Quiet: relearn the resting orientation AND update the noise statistics.
            # Freezing both while over threshold stops a real event from inflating either.
            bx += dx * BASELINE_ALPHA
            by += dy * BASELINE_ALPHA
            bz += dz * BASELINE_ALPHA
            mean += (dyn - mean) * STATS_ALPHA
            mean_sq += (dyn * dyn - mean_sq) * STATS_ALPHA

        if now - last_move < QUIET_S:
            await offer(queue, "vibration", dyn, now_timestamp())  # raw, every sample
        else:
            win_max = max(win_max, dyn)
            if now - win_start >= 1.0:
                await offer(queue, "vibration", win_max, now_timestamp())  # per-second peak
                win_start, win_max = now, 0.0

        await asyncio.sleep(1 / SAMPLE_HZ)


async def readings(queue):
    while True:
        item = await queue.get()
        if item is _SHUTDOWN:
            return  # end the client stream: half-close so the server acks the backlog
        name, value, ts = item
        yield ingest_pb2.Reading(sensor_name=name, value=value, observed_at=ts)


async def streamer(queue, stopping):
    target = os.environ["TARGET"]
    bearer = os.environ["BEARER"]
    metadata = (("authorization", f"Bearer {bearer}"),)
    use_tls = os.environ.get("TLS", "").lower() in ("1", "true", "yes")

    while True:
        try:
            creds_channel = (
                grpc.aio.secure_channel(target, grpc.ssl_channel_credentials())
                if use_tls
                else grpc.aio.insecure_channel(target)
            )
            # The context manager closes the channel on exit; readings() returning
            # on the _SHUTDOWN sentinel half-closes the stream so the server drains
            # and acks the final batch before the channel is torn down.
            async with creds_channel as channel:
                stub = ingest_pb2_grpc.IngestServiceStub(channel)
                print(f"Streaming to {target}")
                await stub.Stream(readings(queue), metadata=metadata)
            return  # readings() only ends via the shutdown sentinel — backlog flushed
        except grpc.aio.AioRpcError as e:
            if stopping.is_set():
                return  # network down mid-shutdown; give up on the remaining backlog
            print(f"stream error: {e.code()} {e.details()}; reconnecting in {RECONNECT_DELAY}s")
            await asyncio.sleep(RECONNECT_DELAY)


def init_sensors():
    temperature_offset = os.environ.get("TEMPERATURE_OFFSET")
    if temperature_offset:
        scd.temperature_offset = int(temperature_offset)

    altitude = os.environ.get("ALTITUDE")
    if altitude:
        scd.altitude = int(altitude)

    print("SCD30 Temperature offset:", scd.temperature_offset)
    print("SCD30 Altitude:", scd.altitude, "meters above sea level")


async def main():
    init_sensors()
    queue = asyncio.Queue(maxsize=QUEUE_MAX)
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stopping.set)

    # Producers feed the queue; the streamer drains it to the server.
    producers = [
        asyncio.create_task(read_sgp40(queue)),
        asyncio.create_task(read_scd30(queue)),
        asyncio.create_task(read_lps22(queue)),
        asyncio.create_task(read_msa311(queue)),
    ]
    stream_task = asyncio.create_task(streamer(queue, stopping))

    # Run until a shutdown signal (or the streamer unexpectedly exits).
    stop_wait = asyncio.create_task(stopping.wait())
    await asyncio.wait({stream_task, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
    stop_wait.cancel()

    # 1. Stop producing first, so the queue stops growing and nothing lands after
    #    the sentinel.
    for p in producers:
        p.cancel()
    await asyncio.gather(*producers, return_exceptions=True)

    # 2. Let the streamer go down last: flush the backlog and half-close the gRPC
    #    stream cleanly. Bounded so a dead network can't stall shutdown.
    async def drain():
        await queue.put(_SHUTDOWN)  # strictly after every producer item
        await stream_task
    try:
        await asyncio.wait_for(drain(), SHUTDOWN_DRAIN_TIMEOUT)
    except TimeoutError:
        pass
    if not stream_task.done():
        stream_task.cancel()
    await asyncio.gather(stream_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
