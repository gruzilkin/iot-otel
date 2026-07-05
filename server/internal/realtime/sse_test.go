package realtime

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gruzilkin/iot-otel/server/internal/hub"
	"github.com/gruzilkin/iot-otel/server/internal/model"
)

type allowAuthorizer struct{}

func (allowAuthorizer) Authorize(context.Context, int64) (bool, error) { return true, nil }

// open issues a streaming GET to the realtime SSE endpoint and returns the open
// response. The caller must read the body to receive events.
func open(t *testing.T, h *hub.Hub, device string) *http.Response {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET /charts/{deviceId}/realtime", NewHandler(h, allowAuthorizer{}, context.Background(), nil))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/charts/"+device+"/realtime", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type: %q", ct)
	}
	return resp
}

func waitForSubscriber(t *testing.T, h *hub.Hub, device int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for h.SubscriberCount(device) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("handler never subscribed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// readDataEvent reads SSE lines until the first `data:` line, returning its payload.
func readDataEvent(t *testing.T, r io.Reader) string {
	t.Helper()
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if payload, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			return payload
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	t.Fatal("no data event received")
	return ""
}

func TestRealtimeDeliversReading(t *testing.T) {
	h := hub.New()
	resp := open(t, h, "7")
	waitForSubscriber(t, h, 7)

	h.Publish(model.Reading{DeviceID: 7, SensorName: "temperature", Value: 21.7896, ObservedAt: time.UnixMilli(1700000000000)})

	data := readDataEvent(t, resp.Body)
	var msg map[string]any
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
	if got := msg["temperature"].(float64); got != 21.789 { // Floor3
		t.Fatalf("want temperature 21.789, got %v", got)
	}
	if got := int64(msg["receivedAt"].(float64)); got != 1700000000000 {
		t.Fatalf("want receivedAt 1700000000000, got %d", got)
	}
}
