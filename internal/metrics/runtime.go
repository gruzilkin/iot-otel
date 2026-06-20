package metrics

import (
	"context"

	"go.opentelemetry.io/otel/metric"

	"github.com/gruzilkin/iot-otel/internal/hub"
	"github.com/gruzilkin/iot-otel/internal/storage"
)

// RegisterRuntime registers operational instruments for the hub and writer so
// failures (DB backlog, slow consumers) are observable before charts look wrong.
func RegisterRuntime(meter metric.Meter, h *hub.Hub, w *storage.BatchWriter) error {
	subscribers, err := meter.Int64ObservableGauge("iot.hub.subscribers",
		metric.WithDescription("Active hub subscriptions."))
	if err != nil {
		return err
	}
	queue, err := meter.Int64ObservableGauge("iot.writer.queue_length",
		metric.WithDescription("Readings buffered awaiting a DB flush."))
	if err != nil {
		return err
	}
	dropped, err := meter.Int64ObservableCounter("iot.hub.dropped",
		metric.WithDescription("Readings dropped to slow consumers."))
	if err != nil {
		return err
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(subscribers, int64(h.TotalSubscribers()))
		o.ObserveInt64(queue, int64(w.QueueLen()))
		o.ObserveInt64(dropped, int64(h.Dropped()))
		return nil
	}, subscribers, queue, dropped)
	return err
}
