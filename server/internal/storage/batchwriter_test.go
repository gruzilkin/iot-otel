package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gruzilkin/iot-otel/server/internal/model"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeDB struct {
	mu    sync.Mutex
	sqls  []string
	calls [][]any
	execd chan struct{}
}

func newFakeDB() *fakeDB { return &fakeDB{execd: make(chan struct{}, 64)} }

func (f *fakeDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	f.sqls = append(f.sqls, sql)
	f.calls = append(f.calls, args)
	f.mu.Unlock()
	f.execd <- struct{}{}
	return pgconn.CommandTag{}, nil
}

func (f *fakeDB) waitExec(t *testing.T) {
	t.Helper()
	select {
	case <-f.execd:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Exec")
	}
}

func (f *fakeDB) execCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestBatchWriterFlushesOnSize(t *testing.T) {
	db := newFakeDB()
	w := NewBatchWriter(db, 3, 16, time.Hour, nil) // latency huge so only size triggers
	defer w.Close(context.Background())

	for i := range 3 {
		if err := w.Enqueue(model.Reading{DeviceID: 1, SensorName: "temperature", Value: float64(i), ObservedAt: time.Unix(int64(i), 0)}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	db.waitExec(t)

	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.calls) != 1 {
		t.Fatalf("want 1 exec, got %d", len(db.calls))
	}
	if got, want := len(db.calls[0]), 3*insertCols; got != want {
		t.Fatalf("want %d bound args, got %d", want, got)
	}
}

func TestBatchWriterFlushesOnLatency(t *testing.T) {
	db := newFakeDB()
	w := NewBatchWriter(db, 100, 16, 25*time.Millisecond, nil) // size huge so only latency triggers
	defer w.Close(context.Background())

	if err := w.Enqueue(model.Reading{DeviceID: 1, SensorName: "voc", Value: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	db.waitExec(t)
	if got := db.execCount(); got != 1 {
		t.Fatalf("want 1 exec, got %d", got)
	}
}

func TestBatchWriterCloseFlushesRemaining(t *testing.T) {
	db := newFakeDB()
	w := NewBatchWriter(db, 100, 16, time.Hour, nil)

	if err := w.Enqueue(model.Reading{DeviceID: 7, SensorName: "ppm", Value: 42}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := db.execCount(); got != 1 {
		t.Fatalf("want 1 exec after close, got %d", got)
	}
}

func TestEnqueueAfterCloseReturnsErrClosed(t *testing.T) {
	db := newFakeDB()
	w := NewBatchWriter(db, 100, 16, time.Hour, nil)
	if err := w.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := w.Enqueue(model.Reading{}); err != ErrClosed {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}
