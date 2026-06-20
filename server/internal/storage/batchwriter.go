package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/gruzilkin/iot-otel/server/internal/model"
)

// ErrClosed is returned by Enqueue after the writer has begun shutting down.
var ErrClosed = errors.New("storage: writer closed")

const (
	insertCols    = 4
	flushTimeout  = 10 * time.Second
	flushAttempts = 2
)

// BatchWriter buffers readings and writes them in batched multi-row INSERTs,
// flushing on either a size threshold or a latency deadline. A single goroutine
// owns the buffer; the input channel is bounded so a stalled database applies
// backpressure to callers of Enqueue rather than growing memory without bound.
type BatchWriter struct {
	db         DB
	ch         chan model.Reading
	maxSize    int
	maxLatency time.Duration
	log        *slog.Logger

	closeOnce sync.Once
	closed    chan struct{}
	doneFlush chan struct{}
}

func NewBatchWriter(db DB, maxSize, queueCap int, maxLatency time.Duration, log *slog.Logger) *BatchWriter {
	if log == nil {
		log = slog.Default()
	}
	w := &BatchWriter{
		db:         db,
		ch:         make(chan model.Reading, queueCap),
		maxSize:    maxSize,
		maxLatency: maxLatency,
		log:        log,
		closed:     make(chan struct{}),
		doneFlush:  make(chan struct{}),
	}
	go w.run()
	return w
}

// QueueLen reports how many readings are buffered awaiting a flush (for
// operational metrics / readiness).
func (w *BatchWriter) QueueLen() int { return len(w.ch) }

// Enqueue blocks while the buffer is full (backpressure) and unblocks if the
// writer closes. The channel is never closed, so concurrent producers can never
// panic on a send.
func (w *BatchWriter) Enqueue(r model.Reading) error {
	select {
	case <-w.closed:
		return ErrClosed
	default:
	}
	select {
	case w.ch <- r:
		return nil
	case <-w.closed:
		return ErrClosed
	}
}

// Close signals shutdown and waits for the final flush (or ctx cancellation).
func (w *BatchWriter) Close(ctx context.Context) error {
	w.closeOnce.Do(func() { close(w.closed) })
	select {
	case <-w.doneFlush:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *BatchWriter) run() {
	defer close(w.doneFlush)

	buf := make([]model.Reading, 0, w.maxSize)
	// NewTimer starts immediately; keep it dormant until the first buffered
	// reading arms it. Since Go 1.23, Stop alone guarantees the channel holds
	// no stale value, so no manual drain is needed.
	timer := time.NewTimer(w.maxLatency)
	timer.Stop()
	timerActive := false

	resetTimer := func() {
		if !timerActive {
			timer.Reset(w.maxLatency)
			timerActive = true
		}
	}
	flush := func() {
		if len(buf) == 0 {
			return
		}
		w.flush(buf)
		buf = buf[:0]
		if timerActive {
			timer.Stop()
			timerActive = false
		}
	}

	for {
		select {
		case r := <-w.ch:
			buf = append(buf, r)
			resetTimer()
			if len(buf) >= w.maxSize {
				flush()
			}
		case <-timer.C:
			timerActive = false
			flush()
		case <-w.closed:
			// Drain anything already buffered in the channel, then flush.
			for {
				select {
				case r := <-w.ch:
					buf = append(buf, r)
					if len(buf) >= w.maxSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

func (w *BatchWriter) flush(batch []model.Reading) {
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()

	var lastErr error
	for range flushAttempts {
		if err := w.exec(ctx, batch); err != nil {
			lastErr = err
			continue
		}
		return
	}
	// Single instance, no broker: a persistent failure means these readings are
	// lost. This is the accepted trade-off; surface it loudly.
	w.log.Error("batch flush failed; dropping readings", "count", len(batch), "err", lastErr)
}

func (w *BatchWriter) exec(ctx context.Context, batch []model.Reading) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO sensor_data (device_id, sensor_name, sensor_value, received_at) VALUES ")
	args := make([]any, 0, len(batch)*insertCols)
	for i := range batch {
		if i > 0 {
			sb.WriteByte(',')
		}
		n := i * insertCols
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d)", n+1, n+2, n+3, n+4)
		args = append(args, batch[i].DeviceID, batch[i].SensorName, batch[i].Value, batch[i].ObservedAt)
	}
	_, err := w.db.Exec(ctx, sb.String(), args...)
	return err
}
