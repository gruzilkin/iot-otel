//go:build integration

package devices_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gruzilkin/iot-otel/server/internal/devices"
	"github.com/gruzilkin/iot-otel/server/internal/testutil"
)

// pg is the shared throwaway Postgres for this package's integration tests. It is
// booted once in TestMain and torn down at the end; each test truncates first.
var pg *testutil.Postgres

var ctx = context.Background()

func TestMain(m *testing.M) {
	p, err := testutil.StartPostgres(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration: start postgres: %v\n", err)
		os.Exit(1)
	}
	pg = p
	code := m.Run()
	_ = pg.Close(ctx)
	os.Exit(code)
}

// --- request wiring -------------------------------------------------------

// ctxUser carries the authenticated user id for fakeUsers to read, so a single
// mux can serve requests as different users without a real session.
type ctxUser struct{}

type fakeUsers struct{}

func (fakeUsers) UserID(c context.Context) (int64, bool) {
	uid, ok := c.Value(ctxUser{}).(int64)
	return uid, ok
}

func asUser(r *http.Request, uid int64) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), ctxUser{}, uid))
}

// newMux wires the real handler→service→repo graph against the shared pool,
// mirroring the routes in cmd/iotd/main.go (minus the session/CSRF middleware,
// which fakeUsers stands in for).
func newMux() *http.ServeMux {
	svc := devices.NewService(devices.NewRepo(pg.Pool))
	h := devices.NewHandler(svc, fakeUsers{}, func(context.Context) string { return "csrf" }, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /devices", h.Index)
	mux.HandleFunc("POST /devices", h.Create)
	mux.HandleFunc("GET /devices/{id}", h.Detail)
	mux.HandleFunc("DELETE /devices/{id}", h.Delete)
	mux.HandleFunc("POST /devices/{id}/tokens", h.AddToken)
	mux.HandleFunc("DELETE /devices/{deviceId}/tokens/{tokenId}", h.DeleteToken)
	return mux
}

func send(mux *http.ServeMux, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func get(mux *http.ServeMux, target string, uid int64) *httptest.ResponseRecorder {
	return send(mux, asUser(httptest.NewRequest(http.MethodGet, target, nil), uid))
}

func del(mux *http.ServeMux, target string, uid int64) *httptest.ResponseRecorder {
	return send(mux, asUser(httptest.NewRequest(http.MethodDelete, target, nil), uid))
}

func post(mux *http.ServeMux, target string, uid int64, form url.Values) *httptest.ResponseRecorder {
	var req *http.Request
	if form != nil {
		req = httptest.NewRequest(http.MethodPost, target, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		req = httptest.NewRequest(http.MethodPost, target, nil)
	}
	return send(mux, asUser(req, uid))
}

// --- fixtures -------------------------------------------------------------

func reset(t *testing.T) {
	t.Helper()
	if err := pg.Truncate(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func seedDevice(t *testing.T, userID int64, name string) int64 {
	t.Helper()
	var id int64
	if err := pg.Pool.QueryRow(ctx,
		`INSERT INTO devices (user_id, name) VALUES ($1, $2) RETURNING device_id`, userID, name).Scan(&id); err != nil {
		t.Fatalf("seed device: %v", err)
	}
	return id
}

func seedToken(t *testing.T, token string, deviceID int64) {
	t.Helper()
	if _, err := pg.Pool.Exec(ctx,
		`INSERT INTO access_tokens (token, device_id, created_at, valid_until)
		 VALUES ($1, $2, now(), now() + interval '1 day')`, token, deviceID); err != nil {
		t.Fatalf("seed token: %v", err)
	}
}

func countTokens(t *testing.T, deviceID int64) int {
	t.Helper()
	var n int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM access_tokens WHERE device_id = $1`, deviceID).Scan(&n); err != nil {
		t.Fatalf("count tokens: %v", err)
	}
	return n
}

// --- tests ----------------------------------------------------------------

func TestCreateDevicePersistsRow(t *testing.T) {
	reset(t)
	rec := post(newMux(), "/devices", 10, url.Values{"name": {"living-room"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "living-room") {
		t.Fatalf("list fragment missing device name: %s", rec.Body.String())
	}
	var uid int64
	if err := pg.Pool.QueryRow(ctx, `SELECT user_id FROM devices WHERE name = 'living-room'`).Scan(&uid); err != nil {
		t.Fatalf("device row not persisted: %v", err)
	}
	if uid != 10 {
		t.Fatalf("device user_id = %d, want 10", uid)
	}
}

func TestCreateRejectsEmptyName(t *testing.T) {
	reset(t)
	rec := post(newMux(), "/devices", 10, url.Values{"name": {"   "}})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
	var n int
	if err := pg.Pool.QueryRow(ctx, `SELECT count(*) FROM devices`).Scan(&n); err != nil {
		t.Fatalf("count devices: %v", err)
	}
	if n != 0 {
		t.Fatalf("no device should be created, found %d", n)
	}
}

func TestAddTokenPersists(t *testing.T) {
	reset(t)
	id := seedDevice(t, 10, "d1")
	rec := post(newMux(), fmt.Sprintf("/devices/%d/tokens", id), 10, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("addtoken status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var token string
	var created, valid time.Time
	if err := pg.Pool.QueryRow(ctx,
		`SELECT token, created_at, valid_until FROM access_tokens WHERE device_id = $1`, id).
		Scan(&token, &created, &valid); err != nil {
		t.Fatalf("token row not persisted: %v", err)
	}
	if len(token) != 32 {
		t.Fatalf("token length = %d, want 32", len(token))
	}
	if !valid.After(created) {
		t.Fatalf("valid_until %v should be after created_at %v", valid, created)
	}
}

// TestForeignUserBlocked proves the ownership boundary lives in real SQL
// (WHERE ... AND user_id = $2), end-to-end — replacing the fake-driven CanAccess test.
func TestForeignUserBlocked(t *testing.T) {
	reset(t)
	id := seedDevice(t, 10, "owned")

	if rec := get(newMux(), fmt.Sprintf("/devices/%d", id), 11); rec.Code != http.StatusNotFound {
		t.Fatalf("foreign GET want 404, got %d", rec.Code)
	}
	if rec := post(newMux(), fmt.Sprintf("/devices/%d/tokens", id), 11, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("foreign AddToken want 404, got %d", rec.Code)
	}
	if n := countTokens(t, id); n != 0 {
		t.Fatalf("foreign AddToken must not insert a token, found %d", n)
	}
}

// TestDeleteCascadesTokens covers the access_tokens FK ON DELETE CASCADE: deleting
// a device removes its tokens. Not observable from HTTP alone, so asserted in the DB.
func TestDeleteCascadesTokens(t *testing.T) {
	reset(t)
	id := seedDevice(t, 10, "owned")
	seedToken(t, "tok-"+strings.Repeat("a", 28), id) // 32 chars total
	if n := countTokens(t, id); n != 1 {
		t.Fatalf("setup: want 1 token, got %d", n)
	}

	rec := del(newMux(), fmt.Sprintf("/devices/%d", id), 10)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status = %d", rec.Code)
	}

	var devCount int
	if err := pg.Pool.QueryRow(ctx, `SELECT count(*) FROM devices WHERE device_id = $1`, id).Scan(&devCount); err != nil {
		t.Fatalf("count devices: %v", err)
	}
	if devCount != 0 {
		t.Fatalf("device not deleted")
	}
	if n := countTokens(t, id); n != 0 {
		t.Fatalf("tokens not cascade-deleted, found %d", n)
	}
}

// TestDeleteMissingDeviceReturnsOK pins the handler's deliberate ErrNotFound swallow:
// deleting a non-existent device still re-renders the list with 200.
func TestDeleteMissingDeviceReturnsOK(t *testing.T) {
	reset(t)
	if rec := del(newMux(), "/devices/999", 10); rec.Code != http.StatusOK {
		t.Fatalf("delete missing want 200 (swallowed), got %d", rec.Code)
	}
}

func TestDeleteTokenRemovesRow(t *testing.T) {
	reset(t)
	id := seedDevice(t, 10, "owned")
	token := "tok-" + strings.Repeat("b", 28) // 32 chars total
	seedToken(t, token, id)

	rec := del(newMux(), fmt.Sprintf("/devices/%d/tokens/%s", id, token), 10)
	if rec.Code != http.StatusOK {
		t.Fatalf("deletetoken status = %d", rec.Code)
	}
	if n := countTokens(t, id); n != 0 {
		t.Fatalf("token not deleted, found %d", n)
	}
}

func TestBadDeviceIDReturns400(t *testing.T) {
	reset(t)
	if rec := get(newMux(), "/devices/abc", 10); rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}
