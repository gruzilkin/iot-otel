package devices

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/gruzilkin/iot-otel/internal/web"
)

const dateLayout = "2006-01-02"

// UserResolver yields the authenticated user id from the request context.
type UserResolver interface {
	UserID(ctx context.Context) (int64, bool)
}

type Handler struct {
	svc   *Service
	users UserResolver
	csrf  func(ctx context.Context) string
	log   *slog.Logger
}

func NewHandler(svc *Service, users UserResolver, csrf func(context.Context) string, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{svc: svc, users: users, csrf: csrf, log: log}
}

// Index renders GET /devices (full page).
func (h *Handler) Index(w http.ResponseWriter, r *http.Request) {
	uid, _ := h.users.UserID(r.Context())
	ds, err := h.svc.List(r.Context(), uid)
	if err != nil {
		h.fail(w, err)
		return
	}
	_ = web.RenderDevicesPage(w, web.DevicesPage{CSRFToken: h.csrf(r.Context()), Devices: deviceViews(ds)})
}

// Create handles POST /devices and returns the refreshed device list fragment.
func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	uid, _ := h.users.UserID(r.Context())
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}
	if _, err := h.svc.Create(r.Context(), uid, name); err != nil {
		h.fail(w, err)
		return
	}
	h.renderList(w, r, uid)
}

// Delete handles DELETE /devices/{id}.
func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	uid, _ := h.users.UserID(r.Context())
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	if err := h.svc.Delete(r.Context(), uid, id); err != nil && !errors.Is(err, ErrNotFound) {
		h.fail(w, err)
		return
	}
	h.renderList(w, r, uid)
}

// Detail handles GET /devices/{id} (edit fragment).
func (h *Handler) Detail(w http.ResponseWriter, r *http.Request) {
	uid, _ := h.users.UserID(r.Context())
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	h.renderEdit(w, r, uid, id)
}

// AddToken handles POST /devices/{id}/tokens.
func (h *Handler) AddToken(w http.ResponseWriter, r *http.Request) {
	uid, _ := h.users.UserID(r.Context())
	id, ok := pathID(w, r, "id")
	if !ok {
		return
	}
	if _, err := h.svc.AddToken(r.Context(), uid, id); err != nil {
		h.handleErr(w, err)
		return
	}
	h.renderEdit(w, r, uid, id)
}

// DeleteToken handles DELETE /devices/{deviceId}/tokens/{tokenId}.
func (h *Handler) DeleteToken(w http.ResponseWriter, r *http.Request) {
	uid, _ := h.users.UserID(r.Context())
	deviceID, ok := pathID(w, r, "deviceId")
	if !ok {
		return
	}
	if err := h.svc.DeleteToken(r.Context(), uid, deviceID, r.PathValue("tokenId")); err != nil {
		h.handleErr(w, err)
		return
	}
	h.renderEdit(w, r, uid, deviceID)
}

func (h *Handler) renderList(w http.ResponseWriter, r *http.Request, uid int64) {
	ds, err := h.svc.List(r.Context(), uid)
	if err != nil {
		h.fail(w, err)
		return
	}
	_ = web.RenderDeviceList(w, web.DeviceListView{Devices: deviceViews(ds)})
}

func (h *Handler) renderEdit(w http.ResponseWriter, r *http.Request, uid, deviceID int64) {
	d, toks, err := h.svc.Get(r.Context(), uid, deviceID)
	if err != nil {
		h.handleErr(w, err)
		return
	}
	_ = web.RenderDeviceEdit(w, web.DeviceEditView{
		Device: web.DeviceView{ID: d.ID, Name: d.Name},
		Tokens: tokenViews(toks),
	})
}

func (h *Handler) handleErr(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	h.fail(w, err)
}

func (h *Handler) fail(w http.ResponseWriter, err error) {
	h.log.Error("devices handler", "err", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

func deviceViews(ds []Device) []web.DeviceView {
	out := make([]web.DeviceView, len(ds))
	for i, d := range ds {
		out[i] = web.DeviceView{ID: d.ID, Name: d.Name}
	}
	return out
}

func tokenViews(ts []Token) []web.TokenView {
	out := make([]web.TokenView, len(ts))
	for i, t := range ts {
		out[i] = web.TokenView{
			Token:      t.Token,
			Created:    t.CreatedAt.Format(dateLayout),
			ValidUntil: t.ValidUntil.Format(dateLayout),
		}
	}
	return out
}

func pathID(w http.ResponseWriter, r *http.Request, key string) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue(key), 10, 64)
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return 0, false
	}
	return id, true
}
