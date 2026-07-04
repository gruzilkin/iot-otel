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
import time

import adafruit_lps2x
import adafruit_scd30
import adafruit_sgp40
import grpc
from adafruit_extended_bus import ExtendedI2C
from adafruit_msa3xx import MSA311, DataRate, Mode, Range
from google.protobuf.timestamp_pb2 import Timestamp

import ingest_pb2
import ingest_pb2_grpc

# Bounded so a server/network stall drops the oldest readings instead of growing
# memory without bound on the Pi.
QUEUE_MAX = 1000
RECONNECT_DELAY = 5

# Open the Linux I2C device directly. Using board.SCL/board.SDA makes Blinka
# perform GPIO board detection, which is unreliable inside a container even when
# /dev/i2c-1 is correctly mapped. Linux controls the bus frequency; the frequency
# argument accepted by CircuitPython busio is not settable through this backend.
i2c = ExtendedI2C(1)
scd = adafruit_scd30.SCD30(i2c)
sgp = adafruit_sgp40.SGP40(i2c)
lps = adafruit_lps2x.LPS22(i2c)  # barometric pressure, default addr 0x5D
msa = MSA311(i2c)  # 3-axis accelerometer, fixed addr 0x62
msa.range = Range.RANGE_4_G
msa.data_rate = DataRate.RATE_125_HZ  # sensor must refresh >= our 100 Hz burst reads
msa.power_mode = Mode.NORMAL

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


# Adaptive accelerometer sampling. The "vibration" signal is the dynamic magnitude
# |accel| - resting gravity, so it rests at ~0. When quiet we sample at IDLE_HZ (to
# react quickly to the onset of shaking) but only emit the per-second peak; on
# movement we switch to ACTIVE_HZ and stream every sample. We fall back to idle only
# after staying active >= MIN_ACTIVE_S AND being quiet for >= QUIET_S (whichever is
# longer).
MOVE_THRESHOLD = 1.5  # m/s^2 deviation from rest that counts as movement
IDLE_HZ = 10
ACTIVE_HZ = 100
MIN_ACTIVE_S = 1.0
QUIET_S = 1.0


def accel_magnitude():
    x, y, z = msa.acceleration
    return (x * x + y * y + z * z) ** 0.5


async def read_msa311(queue):
    baseline = accel_magnitude()  # ~9.81 m/s^2 at rest
    active = False
    active_since = 0.0
    last_move = 0.0
    win_start = time.monotonic()
    win_max = 0.0

    while True:
        now = time.monotonic()
        mag = accel_magnitude()
        dyn = abs(mag - baseline)  # ~0 at rest, spikes on movement
        moving = dyn > MOVE_THRESHOLD

        if moving:
            last_move = now
            if not active:
                active, active_since = True, now
        elif not active:
            baseline += (mag - baseline) * 0.02  # relearn resting orientation, quiet only

        if active:
            await offer(queue, "vibration", dyn, now_timestamp())  # raw, every sample
        else:
            win_max = max(win_max, dyn)
            if now - win_start >= 1.0:
                await offer(queue, "vibration", win_max, now_timestamp())  # per-second peak
                win_start, win_max = now, 0.0

        if active and (now - active_since) >= MIN_ACTIVE_S and (now - last_move) >= QUIET_S:
            active = False

        await asyncio.sleep(1 / ACTIVE_HZ if active else 1 / IDLE_HZ)


async def readings(queue):
    while True:
        name, value, ts = await queue.get()
        yield ingest_pb2.Reading(sensor_name=name, value=value, observed_at=ts)


async def streamer(queue):
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
            async with creds_channel as channel:
                stub = ingest_pb2_grpc.IngestServiceStub(channel)
                print(f"Streaming to {target}")
                await stub.Stream(readings(queue), metadata=metadata)
        except grpc.aio.AioRpcError as e:
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
    await asyncio.gather(
        read_sgp40(queue),
        read_scd30(queue),
        read_lps22(queue),
        read_msa311(queue),
        streamer(queue),
    )


if __name__ == "__main__":
    asyncio.run(main())
