#!/usr/bin/env sh
# Regenerate the Python gRPC stubs from the single source proto for LOCAL dev
# (e.g. running src/sim.py from .venv). The device image generates these the same
# way at build time, so the stubs are intentionally NOT committed.
set -eu
cd "$(dirname "$0")"
.venv/bin/python -m grpc_tools.protoc \
  -I ../proto/ingest/v1 \
  --python_out=src --grpc_python_out=src \
  ingest.proto
echo "generated src/ingest_pb2.py and src/ingest_pb2_grpc.py"
