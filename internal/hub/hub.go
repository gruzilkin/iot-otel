// Package hub is the in-memory pub/sub that replaces RabbitMQ's realtime
// fan-out. Readings are published per device id; subscribers (browser
// WebSocket sessions, the metrics aggregator) each get a buffered channel.
// Publish never blocks: a slow subscriber drops readings rather than stalling
// ingest or other subscribers.
package hub

import (
	"sync"
	"sync/atomic"

	"github.com/gruzilkin/iot-otel/internal/model"
)

const defaultBuffer = 128

type Subscription struct {
	ch      chan model.Reading
	id      uint64
	device  int64
	dropped atomic.Uint64
}

// C is the channel of readings for this subscription. It is closed on
// Unsubscribe.
func (s *Subscription) C() <-chan model.Reading { return s.ch }

// Dropped reports how many readings were dropped because the buffer was full.
func (s *Subscription) Dropped() uint64 { return s.dropped.Load() }

type Hub struct {
	mu     sync.RWMutex
	subs   map[int64]map[uint64]*Subscription
	nextID atomic.Uint64
	buffer int
}

func New() *Hub { return NewWithBuffer(defaultBuffer) }

func NewWithBuffer(buffer int) *Hub {
	return &Hub{subs: make(map[int64]map[uint64]*Subscription), buffer: buffer}
}

func (h *Hub) Subscribe(device int64) *Subscription {
	s := &Subscription{ch: make(chan model.Reading, h.buffer), id: h.nextID.Add(1), device: device}
	h.mu.Lock()
	m := h.subs[device]
	if m == nil {
		m = make(map[uint64]*Subscription)
		h.subs[device] = m
	}
	m[s.id] = s
	h.mu.Unlock()
	return s
}

// Unsubscribe removes the subscription and closes its channel. Closing happens
// under the write lock, so no Publish (which holds the read lock) can be
// mid-send on a closed channel.
func (h *Hub) Unsubscribe(s *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.subs[s.device]
	if m == nil {
		return
	}
	if _, ok := m[s.id]; ok {
		delete(m, s.id)
		close(s.ch)
	}
	if len(m) == 0 {
		delete(h.subs, s.device)
	}
}

func (h *Hub) Publish(r model.Reading) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, s := range h.subs[r.DeviceID] {
		select {
		case s.ch <- r:
		default:
			s.dropped.Add(1)
		}
	}
}

// SubscriberCount returns the number of active subscriptions for a device.
func (h *Hub) SubscriberCount(device int64) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs[device])
}
