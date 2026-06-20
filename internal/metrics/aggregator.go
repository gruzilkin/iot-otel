package metrics

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/gruzilkin/iot-otel/internal/hub"
	"github.com/gruzilkin/iot-otel/internal/model"
)

const defaultIdleTTL = 15 * time.Minute

type key struct {
	device int64
	sensor string
}

type sample struct {
	value float64
	at    time.Time
}

// Aggregator subscribes to all hub readings and exports low-resolution metrics:
// a latest-value observable gauge, a value histogram, and a readings counter,
// keyed by device and sensor. Idle (device,sensor) series are evicted to bound
// cardinality and memory.
type Aggregator struct {
	readings metric.Int64Counter
	values   metric.Float64Histogram

	mu      sync.Mutex
	latest  map[key]sample
	idleTTL time.Duration
	now     func() time.Time
}

func NewAggregator(meter metric.Meter) (*Aggregator, error) {
	readings, err := meter.Int64Counter("sensor.readings.total",
		metric.WithDescription("Total sensor readings ingested."))
	if err != nil {
		return nil, err
	}
	values, err := meter.Float64Histogram("sensor.value",
		metric.WithDescription("Distribution of sensor values per export window."))
	if err != nil {
		return nil, err
	}
	a := &Aggregator{
		readings: readings,
		values:   values,
		latest:   make(map[key]sample),
		idleTTL:  defaultIdleTTL,
		now:      time.Now,
	}
	if _, err := meter.Float64ObservableGauge("sensor.value.latest",
		metric.WithDescription("Most recent value per device and sensor."),
		metric.WithFloat64Callback(a.observeLatest)); err != nil {
		return nil, err
	}
	return a, nil
}

// Run consumes readings until the subscription channel closes.
func (a *Aggregator) Run(sub *hub.Subscription) {
	for r := range sub.C() {
		a.record(r)
	}
}

func (a *Aggregator) record(r model.Reading) {
	attrs := metric.WithAttributes(
		attribute.Int64("device_id", r.DeviceID),
		attribute.String("sensor", r.SensorName),
	)
	a.readings.Add(context.Background(), 1, attrs)
	a.values.Record(context.Background(), r.Value, attrs)

	a.mu.Lock()
	a.latest[key{r.DeviceID, r.SensorName}] = sample{value: r.Value, at: a.now()}
	a.mu.Unlock()
}

func (a *Aggregator) observeLatest(_ context.Context, o metric.Float64Observer) error {
	cutoff := a.now().Add(-a.idleTTL)
	a.mu.Lock()
	defer a.mu.Unlock()
	for k, s := range a.latest {
		if s.at.Before(cutoff) {
			delete(a.latest, k) // evict idle series
			continue
		}
		o.Observe(s.value, metric.WithAttributes(
			attribute.Int64("device_id", k.device),
			attribute.String("sensor", k.sensor),
		))
	}
	return nil
}
