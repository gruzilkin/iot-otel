// Package ingest implements the device-facing gRPC ingestion service.
package ingest

import (
	"errors"
	"io"
	"log/slog"
	"time"

	ingestv1 "github.com/gruzilkin/iot-otel/api/gen/ingest/v1"
	"github.com/gruzilkin/iot-otel/internal/auth"
	"github.com/gruzilkin/iot-otel/internal/model"
	"github.com/gruzilkin/iot-otel/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// allowedSensors bounds what we persist, so a misbehaving device cannot pollute
// sensor_data (and the downstream db_optimizer) with arbitrary series.
var allowedSensors = map[string]struct{}{
	"temperature": {},
	"humidity":    {},
	"voc":         {},
	"ppm":         {},
}

// maxClockSkewFuture / minObserved guard against devices with a wrong clock
// (e.g. a Pi that booted before NTP sync), since timestamps are now device-side.
const maxClockSkewFuture = 24 * time.Hour

var minObserved = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// Publisher receives accepted readings for best-effort realtime fan-out.
// (The in-memory hub satisfies this.)
type Publisher interface {
	Publish(model.Reading)
}

type Service struct {
	ingestv1.UnimplementedIngestServiceServer
	writer    storage.Writer
	publisher Publisher
	log       *slog.Logger
	now       func() time.Time
}

func NewService(writer storage.Writer, publisher Publisher, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{writer: writer, publisher: publisher, log: log, now: time.Now}
}

// Stream consumes the device's reading stream until it half-closes, then returns
// a summary. The device id comes from the auth interceptor, never the payload.
func (s *Service) Stream(stream ingestv1.IngestService_StreamServer) error {
	deviceID, ok := auth.DeviceIDFromContext(stream.Context())
	if !ok {
		return status.Error(codes.Unauthenticated, "missing device identity")
	}

	var accepted, rejected uint64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&ingestv1.StreamSummary{Accepted: accepted, Rejected: rejected})
		}
		if err != nil {
			return err
		}

		r, ok := s.toReading(deviceID, msg)
		if !ok {
			rejected++
			continue
		}
		if err := s.writer.Enqueue(r); err != nil {
			return status.Error(codes.Unavailable, "server shutting down")
		}
		if s.publisher != nil {
			s.publisher.Publish(r) // best-effort realtime fan-out
		}
		accepted++
	}
}

func (s *Service) toReading(deviceID int64, msg *ingestv1.Reading) (model.Reading, bool) {
	name := msg.GetSensorName()
	if _, ok := allowedSensors[name]; !ok {
		return model.Reading{}, false
	}
	ts := msg.GetObservedAt()
	if ts == nil {
		return model.Reading{}, false
	}
	observed := ts.AsTime().UTC()
	if observed.Before(minObserved) || observed.After(s.now().Add(maxClockSkewFuture)) {
		return model.Reading{}, false
	}
	return model.Reading{
		DeviceID:   deviceID,
		SensorName: name,
		Value:      msg.GetValue(),
		ObservedAt: observed,
	}, true
}
