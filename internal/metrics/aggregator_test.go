package metrics

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/gruzilkin/iot-otel/internal/model"
)

func collect(t *testing.T, reader sdkmetric.Reader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func TestAggregatorRecordsMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	agg, err := NewAggregator(mp.Meter("test"))
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}

	now := time.Now()
	agg.now = func() time.Time { return now }
	agg.record(model.Reading{DeviceID: 1, SensorName: "temperature", Value: 21.5, ObservedAt: now})
	agg.record(model.Reading{DeviceID: 1, SensorName: "temperature", Value: 22.0, ObservedAt: now})

	rm := collect(t, reader)

	// Counter: two readings recorded.
	m, ok := findMetric(rm, "sensor.readings.total")
	if !ok {
		t.Fatal("sensor.readings.total missing")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 2 {
		t.Fatalf("unexpected counter data: %#v", m.Data)
	}

	// Observable gauge: latest value is the most recent (22.0).
	g, ok := findMetric(rm, "sensor.value.latest")
	if !ok {
		t.Fatal("sensor.value.latest missing")
	}
	gauge, ok := g.Data.(metricdata.Gauge[float64])
	if !ok || len(gauge.DataPoints) != 1 || gauge.DataPoints[0].Value != 22.0 {
		t.Fatalf("unexpected gauge data: %#v", g.Data)
	}
}

func TestAggregatorEvictsIdleSeries(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	agg, err := NewAggregator(mp.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}

	base := time.Now()
	agg.now = func() time.Time { return base }
	agg.record(model.Reading{DeviceID: 1, SensorName: "temperature", Value: 21.5, ObservedAt: base})

	// Advance past the idle TTL; the gauge callback should evict and emit nothing.
	agg.now = func() time.Time { return base.Add(defaultIdleTTL + time.Minute) }
	rm := collect(t, reader)

	if g, ok := findMetric(rm, "sensor.value.latest"); ok {
		if gauge, ok := g.Data.(metricdata.Gauge[float64]); ok && len(gauge.DataPoints) != 0 {
			t.Fatalf("expected idle series evicted, got %d points", len(gauge.DataPoints))
		}
	}
}
