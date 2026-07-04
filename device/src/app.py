"""Raspberry Pi sensor client: reads SCD30 + SGP40 over I2C and streams readings
to the iotd gRPC ingest service.

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

import adafruit_scd30
import adafruit_sgp40
import grpc
from adafruit_extended_bus import ExtendedI2C
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

temperature, humidity = None, None


async def offer(queue, name, value):
    if queue.full():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
    await queue.put((name, float(value)))


async def read_sgp40(queue):
    while True:
        if temperature is not None and humidity is not None:
            voc_index = sgp.measure_index(temperature=temperature, relative_humidity=humidity)
            if voc_index != 0:
                await offer(queue, "voc", voc_index)
        await asyncio.sleep(1)


async def read_scd30(queue):
    while True:
        if scd.data_available:
            global temperature, humidity
            temperature = scd.temperature
            humidity = scd.relative_humidity
            await offer(queue, "temperature", temperature)
            await offer(queue, "humidity", humidity)
            await offer(queue, "ppm", scd.CO2)
        await asyncio.sleep(2.1)


def now_timestamp():
    ts = Timestamp()
    ts.GetCurrentTime()  # device-side UTC timestamp of the measurement
    return ts


async def readings(queue):
    while True:
        name, value = await queue.get()
        yield ingest_pb2.Reading(sensor_name=name, value=value, observed_at=now_timestamp())


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
    await asyncio.gather(read_sgp40(queue), read_scd30(queue), streamer(queue))


if __name__ == "__main__":
    asyncio.run(main())
