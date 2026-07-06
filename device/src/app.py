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
from dataclasses import dataclass
import os
import signal
import time

import adafruit_lps2x
import adafruit_scd30
import adafruit_sgp40
import grpc
from adafruit_msa3xx import MSA311, DataRate, Mode, Range
from google.protobuf.timestamp_pb2 import Timestamp

import ingest_pb2
import ingest_pb2_grpc
from threadsafe_i2c import ThreadSafeExtendedI2C
from vibration import EventBuffer, VibrationDetector

# Bounded so a server/network stall drops the oldest readings instead of growing
# memory without bound on the Pi.
QUEUE_MAX = 1000
RAW_ACCEL_QUEUE_MAX = 512  # about four seconds at 125 Hz
RECONNECT_DELAY = 5
SHUTDOWN_DRAIN_TIMEOUT = 10  # seconds to flush the backlog on shutdown before giving up

# Enqueued once at shutdown so the streamer flushes the remaining backlog and then
# half-closes the gRPC stream cleanly, rather than being cancelled mid-send.
_SHUTDOWN = object()
_RAW_SHUTDOWN = object()

# Open the Linux I2C device directly. Using board.SCL/board.SDA makes Blinka
# perform GPIO board detection, which is unreliable inside a container even when
# /dev/i2c-1 is correctly mapped. Linux controls the bus frequency; the frequency
# argument accepted by CircuitPython busio is not settable through this backend.
i2c = ThreadSafeExtendedI2C(1)
scd = adafruit_scd30.SCD30(i2c)
sgp = adafruit_sgp40.SGP40(i2c)
lps = adafruit_lps2x.LPS22(i2c)  # barometric pressure, default addr 0x5D
msa = MSA311(i2c)  # 3-axis accelerometer, fixed addr 0x62
# At 125 Hz ODR the MSA311's normal-mode bandwidth is 62.5 Hz. The bandwidth
# register exposed by the shared MSA301/311 driver only controls low-power mode,
# so it is intentionally not set here.
msa.power_mode = Mode.NORMAL
msa.range = Range.RANGE_2_G
msa.data_rate = DataRate.RATE_125_HZ

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
            # The driver waits about 500 ms for conversion. Run it off the event
            # loop so accelerometer sampling continues during that wait.
            voc_index = await asyncio.to_thread(
                sgp.measure_index,
                temperature=temperature,
                relative_humidity=humidity,
            )
            if voc_index != 0:
                await offer(queue, "voc", voc_index, now_timestamp())
        await asyncio.sleep(1)


async def read_scd30(queue):
    while True:
        sample = await asyncio.to_thread(read_scd30_sample)
        if sample is not None:
            global temperature, humidity
            temperature, humidity, co2 = sample
            ts = now_timestamp()
            await offer(queue, "temperature", temperature, ts)
            await offer(queue, "humidity", humidity, ts)
            await offer(queue, "ppm", co2, ts)
        await asyncio.sleep(2.1)


def read_scd30_sample():
    if not scd.data_available:
        return None
    return scd.temperature, scd.relative_humidity, scd.CO2


def now_timestamp():
    ts = Timestamp()
    ts.GetCurrentTime()  # device-side UTC timestamp of the measurement
    return ts


async def read_lps22(queue):
    while True:
        pressure = await asyncio.to_thread(lambda: lps.pressure)
        await offer(queue, "pressure", pressure, now_timestamp())  # hPa
        await asyncio.sleep(1)


# The MSA311 produces one XYZ sample at 125 Hz. We collapse the change from its
# internal resting XYZ vector into one direction-independent magnitude for storage.
SAMPLE_HZ = 125
CALIBRATION_S = 60.0
PRE_TRIGGER_S = 1.0
TAIL_S = 1.0
K_SIGMA = 5.0
STATS_TAU_S = 60.0
BASELINE_TAU_S = 2.0


@dataclass(frozen=True)
class AccelerationSample:
    xyz: tuple[float, float, float]
    monotonic_at: float
    observed_at: Timestamp


async def sample_msa311(raw_queue):
    """Read the MSA311 at a steady rate and enqueue unfiltered XYZ samples."""
    period = 1.0 / SAMPLE_HZ
    deadline = time.monotonic()
    dropped = 0

    while True:
        now = time.monotonic()
        sample = AccelerationSample(msa.acceleration, now, now_timestamp())
        try:
            raw_queue.put_nowait(sample)
        except asyncio.QueueFull:
            # Never stall hardware sampling behind detector work. This should not
            # happen in normal operation; make any loss visible if it does.
            raw_queue.get_nowait()
            raw_queue.put_nowait(sample)
            dropped += 1
            if dropped == 1 or dropped % SAMPLE_HZ == 0:
                print(f"Accelerometer queue full; dropped {dropped} raw samples")

        # Absolute deadlines prevent I2C and scheduler overhead from accumulating
        # into permanent sample-rate drift. Skip missed slots instead of burst-reading
        # duplicate sensor values to catch up.
        deadline += period
        delay = deadline - time.monotonic()
        if delay <= 0:
            deadline += (int(-delay / period) + 1) * period
            await asyncio.sleep(0)
        else:
            await asyncio.sleep(delay)


async def detect_vibration(raw_queue, outgoing_queue):
    """Consume raw XYZ samples and emit only calibrated vibration events."""
    detector = VibrationDetector(
        calibration_seconds=CALIBRATION_S,
        baseline_tau_seconds=BASELINE_TAU_S,
        stats_tau_seconds=STATS_TAU_S,
        tail_seconds=TAIL_S,
        sigma_multiplier=K_SIGMA,
    )
    event_buffer = EventBuffer(round(PRE_TRIGGER_S * SAMPLE_HZ))

    while True:
        sample = await raw_queue.get()
        if sample is _RAW_SHUTDOWN:
            return
        result = detector.process(sample.xyz, sample.monotonic_at)

        if result.just_calibrated:
            print(
                "Vibration detector calibrated:",
                f"mean={detector.noise_mean:.5f}",
                f"sigma={detector.noise_sigma:.5f}",
                f"threshold={detector.threshold:.5f} m/s²",
            )
            event_buffer.clear()
        elif result.ready:
            if result.event_ended:
                await offer(
                    outgoing_queue, "vibration", 0.0, sample.observed_at
                )

            if result.event_started:
                for value, output_ts in event_buffer.flush_with_zero():
                    await offer(outgoing_queue, "vibration", value, output_ts)

            if result.active:
                await offer(
                    outgoing_queue,
                    "vibration",
                    result.value,
                    sample.observed_at,
                )
            else:
                event_buffer.append(result.value, sample.observed_at)


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
    outgoing_queue = asyncio.Queue(maxsize=QUEUE_MAX)
    raw_accel_queue = asyncio.Queue(maxsize=RAW_ACCEL_QUEUE_MAX)
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stopping.set)

    # Hardware readers produce either final sensor readings or raw acceleration.
    sensor_tasks = [
        asyncio.create_task(read_sgp40(outgoing_queue)),
        asyncio.create_task(read_scd30(outgoing_queue)),
        asyncio.create_task(read_lps22(outgoing_queue)),
        asyncio.create_task(sample_msa311(raw_accel_queue)),
    ]
    detector_task = asyncio.create_task(
        detect_vibration(raw_accel_queue, outgoing_queue)
    )
    stream_task = asyncio.create_task(streamer(outgoing_queue, stopping))

    # Run until a shutdown signal (or the streamer unexpectedly exits).
    stop_wait = asyncio.create_task(stopping.wait())
    await asyncio.wait({stream_task, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
    stop_wait.cancel()

    # 1. Stop hardware readers first. Then let the detector consume every raw
    #    sample already queued before it exits.
    for task in sensor_tasks:
        task.cancel()
    await asyncio.gather(*sensor_tasks, return_exceptions=True)
    await raw_accel_queue.put(_RAW_SHUTDOWN)
    await detector_task

    # 2. Let the streamer go down last: flush the outgoing backlog and half-close the gRPC
    #    stream cleanly. Bounded so a dead network can't stall shutdown.
    async def drain():
        await outgoing_queue.put(_SHUTDOWN)  # strictly after every producer item
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
