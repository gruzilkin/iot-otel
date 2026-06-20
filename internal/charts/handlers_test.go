package charts_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gruzilkin/iot-otel/internal/charts"
	"github.com/gruzilkin/iot-otel/internal/sensors"
)

type fakeReader struct {
	data map[string][]sensors.Point
}

func (f fakeReader) ReadData(_ context.Context, _ int64, _ []string, _, _ time.Time, _ int) (map[string][]sensors.Point, error) {
	return f.data, nil
}

func newMux(r charts.Reader) *http.ServeMux {
	h := charts.NewHandler(r, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /charts/{deviceId}", h.Page)
	mux.HandleFunc("GET /charts/{deviceId}/partial", h.Partial)
	return mux
}

func TestChartPageEmbedsData(t *testing.T) {
	r := fakeReader{data: map[string][]sensors.Point{"temperature": {{TimestampMillis: 1, Value: 2}}}}
	rec := httptest.NewRecorder()
	newMux(r).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/charts/1", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"echarts@5.5.1", "const chartData", `"temperature":[[1,2]]`} {
		if !strings.Contains(body, want) {
			t.Fatalf("page missing %q", want)
		}
	}
}

func TestChartPartialJSON(t *testing.T) {
	r := fakeReader{data: map[string][]sensors.Point{"temperature": {{TimestampMillis: 1700000000000, Value: 21.789}}}}
	rec := httptest.NewRecorder()
	newMux(r).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/charts/1/partial?start=0&end=99", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	var got map[string][][]float64
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal %s: %v", rec.Body.String(), err)
	}
	pts := got["temperature"]
	if len(pts) != 1 || pts[0][0] != 1700000000000 || pts[0][1] != 21.789 {
		t.Fatalf("unexpected partial payload: %v", got)
	}
}

func TestChartBadDeviceID(t *testing.T) {
	rec := httptest.NewRecorder()
	newMux(fakeReader{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/charts/notanumber", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}
