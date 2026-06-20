package realtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gruzilkin/iot-otel/server/internal/hub"
	"github.com/gruzilkin/iot-otel/server/internal/model"
)

type allowAuthorizer struct{}

func (allowAuthorizer) Authorize(context.Context, int64) (bool, error) { return true, nil }

func dial(t *testing.T, h *hub.Hub, device string) (*websocket.Conn, context.Context) {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET /charts/{deviceId}/realtime", NewHandler(h, allowAuthorizer{}, nil, nil))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/charts/" + device + "/realtime"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn, ctx
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

func TestRealtimeDeliversReading(t *testing.T) {
	h := hub.New()
	conn, ctx := dial(t, h, "7")
	waitForSubscriber(t, h, 7)

	h.Publish(model.Reading{DeviceID: 7, SensorName: "temperature", Value: 21.7896, ObservedAt: time.UnixMilli(1700000000000)})

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var msg map[string]any
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
	if got := msg["temperature"].(float64); got != 21.789 { // Floor3
		t.Fatalf("want temperature 21.789, got %v", got)
	}
	if got := int64(msg["receivedAt"].(float64)); got != 1700000000000 {
		t.Fatalf("want receivedAt 1700000000000, got %d", got)
	}
}

func TestRealtimePingPong(t *testing.T) {
	h := hub.New()
	conn, ctx := dial(t, h, "7")

	if err := conn.Write(ctx, websocket.MessageText, []byte("ping")); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "pong" {
		t.Fatalf("want pong, got %q", data)
	}
}
