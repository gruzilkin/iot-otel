package hub

import (
	"testing"
	"time"

	"github.com/gruzilkin/iot-otel/internal/model"
)

func reading(device int64, name string) model.Reading {
	return model.Reading{DeviceID: device, SensorName: name, Value: 1, ObservedAt: time.UnixMilli(1)}
}

func TestPublishDeliversToDeviceSubscribers(t *testing.T) {
	h := New()
	sub := h.Subscribe(1)
	defer h.Unsubscribe(sub)

	h.Publish(reading(1, "temperature"))
	select {
	case got := <-sub.C():
		if got.SensorName != "temperature" {
			t.Fatalf("unexpected reading: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("no reading delivered")
	}
}

func TestPublishIsolatedByDevice(t *testing.T) {
	h := New()
	sub := h.Subscribe(1)
	defer h.Unsubscribe(sub)

	h.Publish(reading(2, "temperature")) // different device
	select {
	case got := <-sub.C():
		t.Fatalf("should not receive other device's reading: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestPublishDropsWhenFull(t *testing.T) {
	h := NewWithBuffer(1)
	sub := h.Subscribe(1)
	defer h.Unsubscribe(sub)

	for i := 0; i < 5; i++ {
		h.Publish(reading(1, "ppm")) // never blocks
	}
	if sub.Dropped() != 4 {
		t.Fatalf("want 4 dropped (buffer=1, 5 published), got %d", sub.Dropped())
	}
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	h := New()
	sub := h.Subscribe(1)
	if h.SubscriberCount(1) != 1 {
		t.Fatalf("want 1 subscriber, got %d", h.SubscriberCount(1))
	}
	h.Unsubscribe(sub)
	if _, ok := <-sub.C(); ok {
		t.Fatal("channel should be closed after Unsubscribe")
	}
	if h.SubscriberCount(1) != 0 {
		t.Fatalf("want 0 subscribers, got %d", h.SubscriberCount(1))
	}
}
