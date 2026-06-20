"""Hardware-free device simulator: streams synthetic readings over gRPC so the
ingest service can be exercised end-to-end without a Raspberry Pi.

Env: TARGET (default localhost:50051), BEARER (device token), RATE (seconds, default 1).
"""
import asyncio
import math
import os
import random

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import ingest_pb2
import ingest_pb2_grpc


def now_timestamp():
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


async def readings(rate):
    t = 0
    while True:
        yield ingest_pb2.Reading(sensor_name="temperature", value=round(21 + 2 * math.sin(t / 10), 3), observed_at=now_timestamp())
        yield ingest_pb2.Reading(sensor_name="humidity", value=round(45 + 5 * math.sin(t / 7), 3), observed_at=now_timestamp())
        yield ingest_pb2.Reading(sensor_name="ppm", value=float(600 + random.randint(0, 200)), observed_at=now_timestamp())
        yield ingest_pb2.Reading(sensor_name="voc", value=float(random.randint(10, 80)), observed_at=now_timestamp())
        t += 1
        await asyncio.sleep(rate)


async def main():
    target = os.environ.get("TARGET", "localhost:50051")
    bearer = os.environ.get("BEARER", "")
    rate = float(os.environ.get("RATE", "1"))
    metadata = (("authorization", f"Bearer {bearer}"),)

    async with grpc.aio.insecure_channel(target) as channel:
        stub = ingest_pb2_grpc.IngestServiceStub(channel)
        print(f"Simulating device -> {target} (Ctrl-C to stop)")
        await stub.Stream(readings(rate), metadata=metadata)


if __name__ == "__main__":
    asyncio.run(main())
