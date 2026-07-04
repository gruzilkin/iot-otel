package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"

	"github.com/alexedwards/scs/v2"
)

// Auth bundles the session manager and OAuth provider and exposes the login
// flow, the auth middleware, and CSRF helpers for the web tier.
type Auth struct {
	sessions      *scs.SessionManager
	provider      Provider
	log           *slog.Logger
	loginRedirect string
}

func New(sessions *scs.SessionManager, provider Provider, log *slog.Logger) *Auth {
	if log == nil {
		log = slog.Default()
	}
	return &Auth{sessions: sessions, provider: provider, log: log, loginRedirect: "/devices"}
}

func (a *Auth) Sessions() *scs.SessionManager { return a.sessions }

// UserID returns the authenticated user id from the session, if any.
func (a *Auth) UserID(ctx context.Context) (int64, bool) {
	if !a.sessions.Exists(ctx, sessionUserKey) {
		return 0, false
	}
	return a.sessions.GetInt64(ctx, sessionUserKey), true
}

// Login (GET /login) starts the OAuth dance.
func (a *Auth) Login(w http.ResponseWriter, r *http.Request) {
	if a.provider == nil {
		http.Error(w, "oauth not configured", http.StatusNotImplemented)
		return
	}
	state := randomToken()
	a.sessions.Put(r.Context(), sessionStateKey, state)
	http.Redirect(w, r, a.provider.AuthCodeURL(state), http.StatusFound)
}

// Callback (GET /oauth2/callback) validates state, exchanges the code, resolves
// the user id, and establishes the session.
func (a *Auth) Callback(w http.ResponseWriter, r *http.Request) {
	if a.provider == nil {
		http.Error(w, "oauth not configured", http.StatusNotImplemented)
		return
	}
	want := a.sessions.PopString(r.Context(), sessionStateKey)
	if want == "" || r.URL.Query().Get("state") != want {
		http.Error(w, "invalid oauth state", http.StatusBadRequest)
		return
	}
	tok, err := a.provider.Exchange(r.Context(), r.URL.Query().Get("code"))
	if err != nil {
		a.log.Error("oauth exchange", "err", err)
		http.Error(w, "authentication failed", http.StatusBadGateway)
		return
	}
	uid, err := a.provider.UserID(r.Context(), tok)
	if err != nil {
		a.log.Error("oauth user lookup", "err", err)
		http.Error(w, "authentication failed", http.StatusBadGateway)
		return
	}
	_ = a.sessions.RenewToken(r.Context()) // prevent session fixation
	a.sessions.Put(r.Context(), sessionUserKey, uid)
	http.Redirect(w, r, a.loginRedirect, http.StatusFound)
}

// Logout (POST /logout) destroys the session.
func (a *Auth) Logout(w http.ResponseWriter, r *http.Request) {
	_ = a.sessions.Destroy(r.Context())
	http.Redirect(w, r, "/login", http.StatusFound)
}

// RequireUser redirects unauthenticated requests to /login (for HTML routes).
func (a *Auth) RequireUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := a.UserID(r.Context()); !ok {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RequireUserAPI returns 401 for unauthenticated requests (for JSON/WS routes).
func (a *Auth) RequireUserAPI(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := a.UserID(r.Context()); !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// CSRFToken returns the per-session token, creating it on first use. Templates
// embed it; HTMX sends it back as the X-CSRF-Token header.
func (a *Auth) CSRFToken(ctx context.Context) string {
	t := a.sessions.GetString(ctx, sessionCSRFKey)
	if t == "" {
		t = randomToken()
		a.sessions.Put(ctx, sessionCSRFKey, t)
	}
	return t
}

// RequireCSRF rejects mutating requests whose CSRF token does not match the
// session token. Safe methods pass through.
func (a *Auth) RequireCSRF(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			next.ServeHTTP(w, r)
			return
		}
		want := a.sessions.GetString(r.Context(), sessionCSRFKey)
		if want == "" || !csrfTokenMatches(r, want) {
			http.Error(w, "invalid csrf token", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// csrfTokenMatches accepts the session CSRF token from either the X-CSRF-Token
// header (HTMX sends it via htmx:configRequest) or a csrf_token form field
// (plain, non-HTMX form posts such as logout, which need a full-page redirect
// that HTMX's 302 handling would swallow). The header is checked first so HTMX
// requests never have their body parsed here.
func csrfTokenMatches(r *http.Request, want string) bool {
	if r.Header.Get("X-CSRF-Token") == want {
		return true
	}
	return r.PostFormValue("csrf_token") == want
}

func randomToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return hex.EncodeToString(b)
}
