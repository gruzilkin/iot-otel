package auth

import (
	"net/http"
	"time"

	"github.com/alexedwards/scs/v2"
)

const (
	sessionUserKey  = "userID"
	sessionStateKey = "oauthState"
	sessionCSRFKey  = "csrfToken"
)

// NewSessionManager builds the user session manager. The store is scs's default
// in-memory store: single instance, and sessions are intentionally not persisted
// or revocable. Restart logs everyone out, but because the GitHub OAuth app stays
// authorized, re-login is a silent redirect (no user action) on the next page
// load. A long rolling lifetime keeps an active user signed in indefinitely.
func NewSessionManager() *scs.SessionManager {
	m := scs.New()
	m.Lifetime = 30 * 24 * time.Hour
	m.IdleTimeout = 7 * 24 * time.Hour // rolling: each request extends the session
	m.Cookie.Persist = true            // survive browser restarts
	m.Cookie.HttpOnly = true
	m.Cookie.Secure = true                   // always: the app is served only behind an HTTPS-terminating proxy
	m.Cookie.SameSite = http.SameSiteLaxMode // Lax so the OAuth redirect-back carries the cookie
	return m
}
