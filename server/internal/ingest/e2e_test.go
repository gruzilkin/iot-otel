package ingest_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	ingestv1 "github.com/gruzilkin/iot-otel/server/api/gen/ingest/v1"
	"github.com/gruzilkin/iot-otel/server/internal/auth"
	"github.com/gruzilkin/iot-otel/server/internal/ingest"
	"github.com/gruzilkin/iot-otel/server/internal/model"
	"github.com/gruzilkin/iot-otel/server/internal/storage"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// collectingWriter is a storage.Writer that records what it receives.
type collectingWriter struct {
	mu       sync.Mutex
	readings []model.Reading
}

func (w *collectingWriter) Enqueue(r model.Reading) error {
	w.mu.Lock()
	w.readings = append(w.readings, r)
	w.mu.Unlock()
	return nil
}
func (w *collectingWriter) Close(context.Context) error { return nil }
func (w *collectingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.readings)
}

// fakeTokenDB returns a single valid token row for any token.
type tokenRow struct {
	deviceID   int64
	validUntil time.Time
}

func (r tokenRow) Scan(dest ...any) error {
	*(dest[0].(*int64)) = r.deviceID
	*(dest[1].(*time.Time)) = r.validUntil
	return nil
}

type fakeTokenDB struct{ row tokenRow }

func (d fakeTokenDB) QueryRow(context.Context, string, ...any) pgx.Row { return d.row }

func startServer(t *testing.T, w storage.Writer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	ts := auth.NewTokenStore(fakeTokenDB{row: tokenRow{deviceID: 99, validUntil: time.Now().Add(time.Hour)}}, time.Minute)
	srv := grpc.NewServer(grpc.ChainStreamInterceptor(auth.StreamAuthInterceptor(ts)))
	ingestv1.RegisterIngestServiceServer(srv, ingest.NewService(w, nil, nil))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})
	return conn
}

func TestIngestEndToEnd(t *testing.T) {
	w := &collectingWriter{}
	stub := ingestv1.NewIngestServiceClient(startServer(t, w))

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer device-token")
	stream, err := stub.Stream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := stream.Send(&ingestv1.Reading{SensorName: "temperature", Value: float64(i), ObservedAt: timestamppb.Now()}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	summary, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("close/recv: %v", err)
	}
	if summary.Accepted != 5 {
		t.Fatalf("want 5 accepted, got %d", summary.Accepted)
	}
	if w.count() != 5 {
		t.Fatalf("want 5 persisted, got %d", w.count())
	}
	if got := w.readings[0].DeviceID; got != 99 {
		t.Fatalf("device id should come from token (99), got %d", got)
	}
}

func TestIngestRejectsMissingToken(t *testing.T) {
	stub := ingestv1.NewIngestServiceClient(startServer(t, &collectingWriter{}))

	stream, err := stub.Stream(context.Background()) // no authorization metadata
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	_ = stream.Send(&ingestv1.Reading{SensorName: "temperature", Value: 1, ObservedAt: timestamppb.Now()})
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("want Unauthenticated, got %v", err)
	}
}
