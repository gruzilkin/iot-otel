package sensors

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestFloor3(t *testing.T) {
	cases := []struct {
		in, want float64
	}{
		{1.2349, 1.234},
		{21.7896, 21.789},
		{-0.0001, -0.001},
		{5, 5},
	}
	for _, c := range cases {
		if got := Floor3(c.in); got != c.want {
			t.Errorf("Floor3(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestPointMarshalJSON(t *testing.T) {
	b, err := json.Marshal(Point{TimestampMillis: 1700000000000, Value: 21.789})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "[1700000000000,21.789]" {
		t.Fatalf("got %s", b)
	}
}

type fakeRepo struct{ seen []string }

func (f *fakeRepo) SmartFind(_ context.Context, _ int64, name string, _, _ time.Time, _ int) ([]Point, error) {
	return []Point{{TimestampMillis: 2, Value: 1}, {TimestampMillis: 1, Value: 2}}, nil
}

func TestReadDataFanOut(t *testing.T) {
	s := NewService(&fakeRepo{})
	names := []string{"temperature", "humidity", "voc", "ppm"}
	out, err := s.ReadData(context.Background(), 1, names, time.UnixMilli(0), time.Now(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(names) {
		t.Fatalf("want %d sensors, got %d", len(names), len(out))
	}
	for _, name := range names {
		if len(out[name]) != 2 {
			t.Fatalf("%s: want 2 points, got %d", name, len(out[name]))
		}
	}
}
