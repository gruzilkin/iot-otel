package auth_test

import (
	"context"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/alexedwards/scs/v2"
	"github.com/gruzilkin/iot-otel/internal/auth"
	"golang.org/x/oauth2"
)

type stubProvider struct{ uid int64 }

func (s stubProvider) AuthCodeURL(state string) string {
	return "https://stub.example/auth?state=" + state
}
func (s stubProvider) Exchange(context.Context, string) (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "tok"}, nil
}
func (s stubProvider) UserID(context.Context, *oauth2.Token) (int64, error) { return s.uid, nil }

func newServer(t *testing.T) (*httptest.Server, *http.Client) {
	t.Helper()
	sm := scs.New() // in-memory store for tests
	a := auth.New(sm, stubProvider{uid: 7}, nil)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /login", a.Login)
	mux.HandleFunc("GET /oauth2/callback", a.Callback)
	mux.Handle("GET /dev-login", a.DevLogin(5))
	mux.Handle("GET /protected", a.RequireUser(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid, _ := a.UserID(r.Context())
		_, _ = w.Write([]byte("ok " + strconv.FormatInt(uid, 10)))
	})))
	mux.Handle("GET /api", a.RequireUserAPI(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))
	mux.HandleFunc("GET /csrf", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(a.CSRFToken(r.Context())))
	})
	mux.HandleFunc("POST /mutate", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	srv := httptest.NewServer(sm.LoadAndSave(a.RequireCSRF(mux)))
	t.Cleanup(srv.Close)

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return srv, client
}

func get(t *testing.T, c *http.Client, url string) *http.Response {
	t.Helper()
	resp, err := c.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	resp.Body.Close()
	return resp
}

func TestRequireUserRedirectsAnonymous(t *testing.T) {
	srv, c := newServer(t)
	resp := get(t, c, srv.URL+"/protected")
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "/login" {
		t.Fatalf("want 302 -> /login, got %d -> %q", resp.StatusCode, resp.Header.Get("Location"))
	}
}

func TestRequireUserAPIUnauthorized(t *testing.T) {
	srv, c := newServer(t)
	if resp := get(t, c, srv.URL+"/api"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}

func TestDevLoginEstablishesSession(t *testing.T) {
	srv, c := newServer(t)
	get(t, c, srv.URL+"/dev-login")

	resp, err := c.Get(srv.URL + "/protected")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if got := string(body); got != "ok 5" {
		t.Fatalf("want body 'ok 5', got %q", got)
	}
}

func TestOAuthCallbackFlow(t *testing.T) {
	srv, c := newServer(t)

	resp := get(t, c, srv.URL+"/login")
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("login: want 302, got %d", resp.StatusCode)
	}
	loc, _ := url.Parse(resp.Header.Get("Location"))
	state := loc.Query().Get("state")
	if state == "" {
		t.Fatal("no state in AuthCodeURL")
	}

	resp = get(t, c, srv.URL+"/oauth2/callback?state="+state+"&code=abc")
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "/devices" {
		t.Fatalf("callback: want 302 -> /devices, got %d -> %q", resp.StatusCode, resp.Header.Get("Location"))
	}

	if resp := get(t, c, srv.URL+"/protected"); resp.StatusCode != http.StatusOK {
		t.Fatalf("after login: want 200, got %d", resp.StatusCode)
	}
}

func TestCSRF(t *testing.T) {
	srv, c := newServer(t)

	// No token -> rejected.
	resp, err := c.Post(srv.URL+"/mutate", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("no-token POST: want 403, got %d", resp.StatusCode)
	}

	// Obtain a token (also seeds the session cookie).
	tokenResp, err := c.Get(srv.URL + "/csrf")
	if err != nil {
		t.Fatal(err)
	}
	tokenBytes, _ := io.ReadAll(tokenResp.Body)
	tokenResp.Body.Close()
	token := string(tokenBytes)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mutate", nil)
	req.Header.Set("X-CSRF-Token", token)
	resp2, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("valid-token POST: want 204, got %d", resp2.StatusCode)
	}
}
