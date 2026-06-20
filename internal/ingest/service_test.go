package ingest

import (
	"context"
	"io"
	"testing"
	"time"

	ingestv1 "github.com/gruzilkin/iot-otel/api/gen/ingest/v1"
	"github.com/gruzilkin/iot-otel/internal/auth"
	"github.com/gruzilkin/iot-otel/internal/model"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeWriter struct {
	readings []model.Reading
}

func (w *fakeWriter) Enqueue(r model.Reading) error { w.readings = append(w.readings, r); return nil }
func (w *fakeWriter) Close(context.Context) error   { return nil }

type fakeStream struct {
	grpc.ServerStream
	ctx     context.Context
	recvs   []*ingestv1.Reading
	i       int
	summary *ingestv1.StreamSummary
}

func (f *fakeStream) Context() context.Context { return f.ctx }

func (f *fakeStream) Recv() (*ingestv1.Reading, error) {
	if f.i >= len(f.recvs) {
		return nil, io.EOF
	}
	r := f.recvs[f.i]
	f.i++
	return r, nil
}

func (f *fakeStream) SendAndClose(s *ingestv1.StreamSummary) error {
	f.summary = s
	return nil
}

var testNow = time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)

func reading(name string, val float64, ts time.Time) *ingestv1.Reading {
	return &ingestv1.Reading{SensorName: name, Value: val, ObservedAt: timestamppb.New(ts)}
}

func runStream(t *testing.T, msgs []*ingestv1.Reading) (*fakeWriter, *ingestv1.StreamSummary) {
	t.Helper()
	w := &fakeWriter{}
	svc := NewService(w, nil)
	svc.now = func() time.Time { return testNow }
	stream := &fakeStream{ctx: auth.WithDeviceID(context.Background(), 99), recvs: msgs}
	if err := svc.Stream(stream); err != nil {
		t.Fatalf("Stream returned error: %v", err)
	}
	return w, stream.summary
}

func TestStreamAcceptsValidReadings(t *testing.T) {
	w, summary := runStream(t, []*ingestv1.Reading{
		reading("temperature", 21.5, testNow),
		reading("ppm", 640, testNow.Add(-time.Second)),
	})
	if summary.Accepted != 2 || summary.Rejected != 0 {
		t.Fatalf("want accepted=2 rejected=0, got %+v", summary)
	}
	if len(w.readings) != 2 {
		t.Fatalf("want 2 persisted, got %d", len(w.readings))
	}
	if w.readings[0].DeviceID != 99 {
		t.Fatalf("device id not taken from context: %d", w.readings[0].DeviceID)
	}
}

func TestStreamRejectsUnknownSensorAndBadClock(t *testing.T) {
	w, summary := runStream(t, []*ingestv1.Reading{
		reading("co2_pretend", 1, testNow),                                  // not in allow-list
		reading("temperature", 1, testNow.Add(48*time.Hour)),                // too far future
		reading("humidity", 1, time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)), // pre-2020
		reading("voc", 30, testNow),                                         // valid
	})
	if summary.Accepted != 1 || summary.Rejected != 3 {
		t.Fatalf("want accepted=1 rejected=3, got %+v", summary)
	}
	if len(w.readings) != 1 || w.readings[0].SensorName != "voc" {
		t.Fatalf("unexpected persisted readings: %+v", w.readings)
	}
}

func TestStreamRequiresDeviceIdentity(t *testing.T) {
	svc := NewService(&fakeWriter{}, nil)
	stream := &fakeStream{ctx: context.Background()} // no device id injected
	if err := svc.Stream(stream); err == nil {
		t.Fatal("expected Unauthenticated error, got nil")
	}
}
