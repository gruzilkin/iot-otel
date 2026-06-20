// Package metrics exports low-resolution OpenTelemetry metrics derived from the
// sensor stream, plus operational instruments. The OTLP destination is the
// operator's concern, configured entirely via standard OTEL_* env vars.
package metrics

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Enabled reports whether an OTLP endpoint is configured. When false, metrics
// are skipped so local runs don't spam connection failures.
func Enabled() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != ""
}

// NewMeterProvider builds a periodic-push OTLP MeterProvider self-configured
// from the standard OTEL_* environment variables (endpoint, headers, protocol,
// interval, resource attributes).
func NewMeterProvider(ctx context.Context) (*sdkmetric.MeterProvider, error) {
	exp, err := newExporter(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithFromEnv(), resource.WithTelemetrySDK())
	if err != nil {
		return nil, err
	}
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)),
		sdkmetric.WithResource(res),
	), nil
}

func newExporter(ctx context.Context) (sdkmetric.Exporter, error) {
	proto := os.Getenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")
	if proto == "" {
		proto = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
	}
	if strings.HasPrefix(proto, "http") {
		return otlpmetrichttp.New(ctx)
	}
	return otlpmetricgrpc.New(ctx)
}
