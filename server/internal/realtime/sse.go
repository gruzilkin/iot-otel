// Package realtime serves live sensor readings to browsers over Server-Sent
// Events (SSE), sourced from the in-memory hub.
package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gruzilkin/iot-otel/server/internal/hub"
	"github.com/gruzilkin/iot-otel/server/internal/sensors"
)

const (
	writeTimeout  = 10 * time.Second
	heartbeatTick = 30 * time.Second
)

// Authorizer reports whether the current request may access a device.
type Authorizer interface {
	Authorize(ctx context.Context, deviceID int64) (bool, error)
}

// Handler serves GET /charts/{deviceId}/realtime as an SSE stream.
type Handler struct {
	hub   *hub.Hub
	authz Authorizer
	// shutdown is cancelled when the server begins shutting down. SSE streams are
	// long-lived, so without a server-wide signal they'd hold http.Server.Shutdown
	// open until its timeout (Shutdown waits for handlers to return but does not
	// cancel their request contexts).
	shutdown context.Context
	log      *slog.Logger
}

func NewHandler(h *hub.Hub, authz Authorizer, shutdown context.Context, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	if shutdown == nil {
		shutdown = context.Background()
	}
	return &Handler{hub: h, authz: authz, shutdown: shutdown, log: log}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	deviceID, err := strconv.ParseInt(r.PathValue("deviceId"), 10, 64)
	if err != nil {
		http.Error(w, "bad device id", http.StatusBadRequest)
		return
	}
	allowed, err := h.authz.Authorize(r.Context(), deviceID)
	if err != nil {
		h.log.Error("authorize", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !allowed {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // defeat proxy response buffering
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		return // ResponseWriter does not support streaming
	}

	sub, unsubscribe := h.hub.Subscribe(deviceID)
	defer unsubscribe()

	// EventSource auto-reconnects, so a half-open connection is detected on the
	// next write. The heartbeat comment keeps idle proxies from dropping the
	// stream and surfaces a write error when nothing has been published lately.
	ping := time.NewTicker(heartbeatTick)
	defer ping.Stop()

	for {
		select {
		case <-h.shutdown.Done():
			return // server is shutting down; release the stream so Shutdown can finish
		case <-r.Context().Done():
			return
		case <-ping.C:
			if !h.write(rc, w, ": ping\n\n") {
				return
			}
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
			if !h.write(rc, w, fmt.Sprintf("data: %s\n\n", payload)) {
				return
			}
		}
	}
}

// write sends one SSE chunk with a per-write deadline (slow-client protection)
// and flushes it. It reports whether the write succeeded.
func (h *Handler) write(rc *http.ResponseController, w io.Writer, s string) bool {
	_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
	if _, err := io.WriteString(w, s); err != nil {
		return false
	}
	return rc.Flush() == nil
}
