//go:build integration

package sensors

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gruzilkin/iot-otel/server/internal/testutil"
)

var integrationDB *testutil.Postgres

func TestMain(m *testing.M) {
	ctx := context.Background()
	db, err := testutil.StartPostgres(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration: start postgres: %v\n", err)
		os.Exit(1)
	}
	integrationDB = db
	code := m.Run()
	_ = integrationDB.Close(ctx)
	os.Exit(code)
}

func TestSmartFindEndpointsUseTimestampOrder(t *testing.T) {
	ctx := context.Background()
	if err := integrationDB.Truncate(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	var deviceID int64
	if err := integrationDB.Pool.QueryRow(ctx,
		`INSERT INTO devices (user_id, name) VALUES (1, 'test') RETURNING device_id`).Scan(&deviceID); err != nil {
		t.Fatalf("insert device: %v", err)
	}

	base := time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)
	for _, point := range []struct {
		value float64
		at    time.Time
	}{
		{20, base.Add(time.Minute)},
		{30, base.Add(2 * time.Minute)},
		{10, base},
	} {
		if _, err := integrationDB.Pool.Exec(ctx,
			`INSERT INTO sensor_data (device_id, sensor_name, sensor_value, received_at)
			 VALUES ($1, 'temperature', $2, $3)`, deviceID, point.value, point.at); err != nil {
			t.Fatalf("insert sensor point: %v", err)
		}
	}

	points, err := NewPgxRepo(integrationDB.Pool).SmartFind(
		ctx, deviceID, "temperature", base.Add(-time.Minute), base.Add(3*time.Minute), 0,
	)
	if err != nil {
		t.Fatalf("SmartFind: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("got %d endpoints, want 2: %v", len(points), points)
	}
	if points[0].TimestampMillis != base.UnixMilli() || points[0].Value != 10 {
		t.Fatalf("first endpoint = %+v, want earliest timestamp/value", points[0])
	}
	if points[1].TimestampMillis != base.Add(2*time.Minute).UnixMilli() || points[1].Value != 30 {
		t.Fatalf("last endpoint = %+v, want latest timestamp/value", points[1])
	}
}

func TestSchemaIncludesStreamTimeIndex(t *testing.T) {
	var definition string
	if err := integrationDB.Pool.QueryRow(context.Background(),
		`SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'sensor_data_stream_time_idx'`).Scan(&definition); err != nil {
		t.Fatalf("read stream/time index: %v", err)
	}
	for _, want := range []string{"device_id", "sensor_name", "received_at", "id", "INCLUDE (sensor_value)"} {
		if !strings.Contains(definition, want) {
			t.Fatalf("index definition %q missing %q", definition, want)
		}
	}
}
