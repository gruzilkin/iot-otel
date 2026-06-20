# Multi-stage build for the iotd server.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /iotd ./cmd/iotd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /iotd /iotd
EXPOSE 8080 50051
USER nonroot:nonroot
ENTRYPOINT ["/iotd"]
