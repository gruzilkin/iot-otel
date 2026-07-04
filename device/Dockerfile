# Raspberry Pi sensor client (gRPC). Built from the REPO ROOT (not ./device) so the
# single source proto can be compiled into Python stubs at build time:
#   docker compose -f docker-compose.device.yml up --build
# (the compose file sets context: . and dockerfile: device/Dockerfile).

# Stage 1: generate the gRPC stubs from the top-level proto. Isolating this keeps
# grpcio-tools (and the proto) out of the final image, and guarantees the stubs are
# always in sync with proto/ — they are intentionally not committed.
FROM python:3.12-slim AS protogen
WORKDIR /gen
RUN pip3 install --no-cache-dir grpcio-tools==1.81.1
COPY proto/ingest/v1/ingest.proto .
RUN python3 -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. ingest.proto

FROM python:3.12-slim
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends i2c-tools \
    && rm -rf /var/lib/apt/lists/*

COPY device/requirements.stable.txt requirements.stable.txt
RUN pip3 install -r requirements.stable.txt

COPY device/requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY device/src/ .
COPY --from=protogen /gen/ingest_pb2.py /gen/ingest_pb2_grpc.py ./

CMD ["python3", "app.py"]
