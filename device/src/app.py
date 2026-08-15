"""Raspberry Pi sensor client: reads SCD30, SGP40 and LPS22 over I2C and
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
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import adafruit_lps2x
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

# Every sensor read runs on this single worker thread: slow driver waits (the
# SGP40 conversion takes ~500 ms) stay off the event loop, and one worker
# serializes all bus transactions by construction — Blinka's cooperative bus
# lock is not thread-safe, so overlapping reads from a thread pool would race.
_i2c_executor = ThreadPoolExecutor(max_workers=1)


async def read_sensor(fn, *args, **kwargs):
    return await asyncio.get_running_loop().run_in_executor(
        _i2c_executor, partial(fn, *args, **kwargs)
    )


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
            voc_index = await read_sensor(
                sgp.measure_index,
                temperature=temperature,
                relative_humidity=humidity,
            )
            if voc_index != 0:
                await offer(queue, "voc", voc_index, now_timestamp())
        await asyncio.sleep(1)


async def read_scd30(queue):
    while True:
        sample = await read_sensor(read_scd30_sample)
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
        pressure = await read_sensor(lambda: lps.pressure)
        await offer(queue, "pressure", pressure, now_timestamp())  # hPa
        await asyncio.sleep(1)


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
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stopping.set)

    sensor_tasks = [
        asyncio.create_task(read_sgp40(outgoing_queue)),
        asyncio.create_task(read_scd30(outgoing_queue)),
        asyncio.create_task(read_lps22(outgoing_queue)),
    ]
    stream_task = asyncio.create_task(streamer(outgoing_queue, stopping))

    # Run until a shutdown signal (or the streamer unexpectedly exits).
    stop_wait = asyncio.create_task(stopping.wait())
    await asyncio.wait({stream_task, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
    stop_wait.cancel()

    # 1. Stop hardware readers first, so nothing new enters the outgoing queue.
    for task in sensor_tasks:
        task.cancel()
    await asyncio.gather(*sensor_tasks, return_exceptions=True)

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
