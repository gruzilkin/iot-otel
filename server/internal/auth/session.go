package auth

import (
	"net/http"
	"time"

	"github.com/alexedwards/scs/pgxstore"
	"github.com/alexedwards/scs/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sessionUserKey  = "userID"
	sessionStateKey = "oauthState"
	sessionCSRFKey  = "csrfToken"
)

// NewSessionManager builds a server-side (revocable) session manager backed by
// the Postgres sessions table.
func NewSessionManager(pool *pgxpool.Pool) *scs.SessionManager {
	m := scs.New()
	m.Store = pgxstore.New(pool)
	m.Lifetime = 7 * 24 * time.Hour
	m.Cookie.HttpOnly = true
	m.Cookie.Secure = true                   // always: the app is served only behind an HTTPS-terminating proxy
	m.Cookie.SameSite = http.SameSiteLaxMode // Lax so the OAuth redirect-back carries the cookie
	return m
}
