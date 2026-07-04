package metrics

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// LogsEnabled reports whether an OTLP endpoint is configured for logs. When
// false, log export is skipped and the app logs only to stdout (mirrors
// Enabled for metrics).
func LogsEnabled() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT") != ""
}

// NewLoggerProvider builds a batch-exporting OTLP LoggerProvider self-configured
// from the standard OTEL_* environment variables (endpoint, headers, protocol,
// resource attributes). Shutdown flushes buffered records, so callers must defer
// it. The logs signal is still beta in the OTel Go SDK.
func NewLoggerProvider(ctx context.Context) (*sdklog.LoggerProvider, error) {
	exp, err := newLogExporter(ctx)
	if err != nil {
		return nil, err
	}
	res, err := newResource(ctx)
	if err != nil {
		return nil, err
	}
	return sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
	), nil
}

// newLogExporter selects the OTLP transport the same way newExporter does for
// metrics: the exporter packages pick the transport by import (they do not honor
// OTEL_EXPORTER_OTLP_PROTOCOL themselves), so we branch on the protocol env var
// here. http* → HTTP/protobuf, otherwise gRPC.
func newLogExporter(ctx context.Context) (sdklog.Exporter, error) {
	proto := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL")
	if proto == "" {
		proto = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
	}
	if strings.HasPrefix(proto, "http") {
		return otlploghttp.New(ctx)
	}
	return otlploggrpc.New(ctx)
}
