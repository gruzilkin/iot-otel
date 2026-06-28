// Package hub is the in-memory pub/sub that replaces RabbitMQ's realtime
// fan-out. Readings are published per device id; subscribers (browser
// SSE sessions, the metrics aggregator) each get a buffered channel.
// Publish never blocks: a slow subscriber drops readings rather than stalling
// ingest or other subscribers.
package hub

import (
	"sync"
	"sync/atomic"

	"github.com/gruzilkin/iot-otel/server/internal/model"
)

const defaultBuffer = 128

type Subscription struct {
	ch      chan model.Reading
	device  int64
	dropped atomic.Uint64
}

// C is the channel of readings for this subscription. It is closed by the
// cleanup func returned by Subscribe.
func (s *Subscription) C() <-chan model.Reading { return s.ch }

// Dropped reports how many readings were dropped because the buffer was full.
func (s *Subscription) Dropped() uint64 { return s.dropped.Load() }

type Hub struct {
	mu      sync.RWMutex
	subs    map[int64]map[*Subscription]struct{} // per-device subscribers
	all     map[*Subscription]struct{}           // all-device subscribers (e.g. metrics)
	buffer  int
	dropped atomic.Uint64 // hub-wide dropped readings (slow consumers)
}

func New() *Hub { return NewWithBuffer(defaultBuffer) }

func NewWithBuffer(buffer int) *Hub {
	return &Hub{
		subs:   make(map[int64]map[*Subscription]struct{}),
		all:    make(map[*Subscription]struct{}),
		buffer: buffer,
	}
}

// Subscribe registers a subscriber for one device's readings. It returns the
// subscription and a cleanup func that removes it and closes its channel. The
// caller must call cleanup when done (typically via defer); otherwise the
// subscription leaks and Publish keeps fanning out to a channel no one reads.
// cleanup is safe to call more than once.
func (h *Hub) Subscribe(device int64) (*Subscription, func()) {
	s := &Subscription{ch: make(chan model.Reading, h.buffer), device: device}
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.subs[device]
	if m == nil {
		m = make(map[*Subscription]struct{})
		h.subs[device] = m
	}
	m[s] = struct{}{}
	return s, func() { h.unsubscribe(s) }
}

// SubscribeAll subscribes to readings from every device (used by the metrics
// aggregator). It uses a larger buffer since it sees all device traffic. Unlike
// Subscribe it returns no cleanup func: the aggregator lives for the process
// lifetime, so an all-subscription is never individually removed.
func (h *Hub) SubscribeAll() *Subscription {
	s := &Subscription{ch: make(chan model.Reading, h.buffer*4)}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.all[s] = struct{}{}
	return s
}

// unsubscribe removes a device subscription and closes its channel; it is the
// cleanup func returned by Subscribe. Closing happens under the write lock, so
// no Publish (which holds the read lock) can be mid-send on a closed channel.
// Calling it again after the subscription is gone is a no-op.
func (h *Hub) unsubscribe(s *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.subs[s.device]
	if m == nil {
		return
	}
	if _, ok := m[s]; ok {
		delete(m, s)
		close(s.ch)
	}
	if len(m) == 0 {
		delete(h.subs, s.device)
	}
}

func (h *Hub) Publish(r model.Reading) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs[r.DeviceID] {
		h.send(s, r)
	}
	for s := range h.all {
		h.send(s, r)
	}
}

func (h *Hub) send(s *Subscription, r model.Reading) {
	select {
	case s.ch <- r:
	default:
		s.dropped.Add(1)
		h.dropped.Add(1)
	}
}

// SubscriberCount returns the number of active subscriptions for a device.
func (h *Hub) SubscriberCount(device int64) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs[device])
}

// TotalSubscribers returns the number of active subscriptions across all devices
// (including all-device subscribers).
func (h *Hub) TotalSubscribers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := len(h.all)
	for _, m := range h.subs {
		n += len(m)
	}
	return n
}

// Dropped returns the cumulative number of readings dropped to slow consumers.
func (h *Hub) Dropped() uint64 { return h.dropped.Load() }
