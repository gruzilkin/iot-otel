// Package realtime serves live sensor readings to browsers over WebSocket,
// sourced from the in-memory hub.
package realtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/gruzilkin/iot-otel/internal/hub"
	"github.com/gruzilkin/iot-otel/internal/sensors"
)

const writeTimeout = 10 * time.Second

// Handler serves GET /charts/{deviceId}/realtime.
type Handler struct {
	hub            *hub.Hub
	log            *slog.Logger
	originPatterns []string
}

func NewHandler(h *hub.Hub, originPatterns []string, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{hub: h, log: log, originPatterns: originPatterns}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	deviceID, err := strconv.ParseInt(r.PathValue("deviceId"), 10, 64)
	if err != nil {
		http.Error(w, "bad device id", http.StatusBadRequest)
		return
	}
	// TODO(phase4): authorize the session user against this device before upgrade.

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: h.originPatterns})
	if err != nil {
		return // Accept already wrote the response
	}
	defer conn.CloseNow()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	sub := h.hub.Subscribe(deviceID)
	defer h.hub.Unsubscribe(sub)

	go h.readPump(ctx, cancel, conn)
	h.writePump(ctx, conn, sub)
}

// readPump handles the keepalive ("ping"->"pong") and ends the connection on
// any other client message or read error, cancelling the write pump.
func (h *Handler) readPump(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn) {
	defer cancel()
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ == websocket.MessageText && string(data) == "ping" {
			writeCtx, c := context.WithTimeout(ctx, writeTimeout)
			_ = conn.Write(writeCtx, websocket.MessageText, []byte("pong"))
			c()
			continue
		}
		conn.Close(websocket.StatusUnsupportedData, "unexpected message")
		return
	}
}

func (h *Handler) writePump(ctx context.Context, conn *websocket.Conn, sub *hub.Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case reading, ok := <-sub.C():
			if !ok {
				return
			}
			payload, err := json.Marshal(map[string]any{
				reading.SensorName: sensors.Floor3(reading.Value),
				"receivedAt":       reading.ObservedAt.UnixMilli(),
			})
			if err != nil {
				continue
			}
			writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
			err = conn.Write(writeCtx, websocket.MessageText, payload)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
