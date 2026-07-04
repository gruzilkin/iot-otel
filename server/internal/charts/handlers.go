// Package charts serves the historical chart page and its JSON data endpoint.
package charts

import (
	"context"
	"encoding/json"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gruzilkin/iot-otel/server/internal/sensors"
	"github.com/gruzilkin/iot-otel/server/internal/web"
)

// SensorNames is the fixed set of series the chart renders.
var SensorNames = []string{"temperature", "humidity", "voc", "ppm", "pressure", "vibration"}

const defaultLimit = 1000

type Reader interface {
	ReadData(ctx context.Context, deviceID int64, names []string, start, end time.Time, limit int) (map[string][]sensors.Point, error)
}

// Authorizer reports whether the current request may access a device.
type Authorizer interface {
	Authorize(ctx context.Context, deviceID int64) (bool, error)
}

type Handler struct {
	reader Reader
	authz  Authorizer
	log    *slog.Logger
}

func NewHandler(reader Reader, authz Authorizer, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{reader: reader, authz: authz, log: log}
}

// authorize returns true if the request may proceed; otherwise it has already
// written the appropriate error response.
func (h *Handler) authorize(w http.ResponseWriter, r *http.Request, deviceID int64) bool {
	ok, err := h.authz.Authorize(r.Context(), deviceID)
	if err != nil {
		h.log.Error("authorize", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return false
	}
	if !ok {
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}
	return true
}

// Page renders GET /charts/{deviceId} with the initial series embedded.
func (h *Handler) Page(w http.ResponseWriter, r *http.Request) {
	deviceID, ok := deviceIDFromPath(w, r)
	if !ok {
		return
	}
	if !h.authorize(w, r, deviceID) {
		return
	}
	data, err := h.read(r.Context(), deviceID, time.UnixMilli(0), time.Now())
	if err != nil {
		h.log.Error("read chart data", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	js, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := web.RenderChart(w, web.ChartPage{DeviceID: deviceID, Data: template.JS(js)}); err != nil {
		h.log.Error("render chart", "err", err)
	}
}

// Partial serves GET /charts/{deviceId}/partial?start&end as JSON.
func (h *Handler) Partial(w http.ResponseWriter, r *http.Request) {
	deviceID, ok := deviceIDFromPath(w, r)
	if !ok {
		return
	}
	if !h.authorize(w, r, deviceID) {
		return
	}
	start := queryMillis(r, "start", time.UnixMilli(0))
	end := queryMillis(r, "end", time.Now())
	data, err := h.read(r.Context(), deviceID, start, end)
	if err != nil {
		h.log.Error("read chart data", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.log.Error("encode partial", "err", err)
	}
}

func (h *Handler) read(ctx context.Context, deviceID int64, start, end time.Time) (map[string][]sensors.Point, error) {
	return h.reader.ReadData(ctx, deviceID, SensorNames, start, end, defaultLimit)
}

func deviceIDFromPath(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("deviceId"), 10, 64)
	if err != nil {
		http.Error(w, "bad device id", http.StatusBadRequest)
		return 0, false
	}
	return id, true
}

func queryMillis(r *http.Request, key string, def time.Time) time.Time {
	if v := r.URL.Query().Get(key); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.UnixMilli(ms)
		}
	}
	return def
}
