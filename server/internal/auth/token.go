// Package auth handles device authentication for the gRPC ingest path.
// (User/session auth for the web tier is added later, in this same package.)
package auth

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrNoToken is returned when the bearer token is missing or malformed.
	ErrNoToken = errors.New("auth: missing or malformed bearer token")
	// ErrTokenUnknown is returned when the token is not in access_tokens.
	ErrTokenUnknown = errors.New("auth: token not found")
	// ErrTokenExpired is returned when valid_until has passed.
	ErrTokenExpired = errors.New("auth: token expired")
)

// Querier is the subset of pgxpool.Pool the token store needs.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const tokenQuery = `SELECT device_id, valid_until FROM access_tokens WHERE token = $1`

// TokenStore resolves a device bearer token to a device id, enforcing
// valid_until (which the legacy WebSocket handler never checked). A short TTL
// cache avoids a DB round-trip on every reconnect; the cost is that a deleted or
// expired token can stay accepted until the cache entry expires.
type TokenStore struct {
	db  Querier
	ttl time.Duration
	now func() time.Time

	mu    sync.Mutex
	cache map[string]tokenEntry
}

type tokenEntry struct {
	deviceID   int64
	validUntil time.Time
	fetchedAt  time.Time
}

func NewTokenStore(db Querier, ttl time.Duration) *TokenStore {
	return &TokenStore{
		db:    db,
		ttl:   ttl,
		now:   time.Now,
		cache: make(map[string]tokenEntry),
	}
}

// Lookup returns the device id for a token, or one of the Err* sentinels.
func (s *TokenStore) Lookup(ctx context.Context, token string) (int64, error) {
	now := s.now()
	if e, ok := s.cached(token, now); ok {
		return validate(e, now)
	}

	var e tokenEntry
	err := s.db.QueryRow(ctx, tokenQuery, token).Scan(&e.deviceID, &e.validUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrTokenUnknown
	}
	if err != nil {
		return 0, err
	}
	e.fetchedAt = now
	s.store(token, e)
	return validate(e, now)
}

func validate(e tokenEntry, now time.Time) (int64, error) {
	if !e.validUntil.After(now) {
		return 0, ErrTokenExpired
	}
	return e.deviceID, nil
}

func (s *TokenStore) cached(token string, now time.Time) (tokenEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.cache[token]
	if !ok || now.Sub(e.fetchedAt) >= s.ttl {
		return tokenEntry{}, false
	}
	return e, true
}

func (s *TokenStore) store(token string, e tokenEntry) {
	s.mu.Lock()
	s.cache[token] = e
	s.mu.Unlock()
}
