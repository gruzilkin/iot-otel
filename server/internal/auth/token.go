// Package auth handles device authentication for the gRPC ingest path.
// (User/session auth for the web tier is added later, in this same package.)
package auth

import (
	"context"
	"errors"
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
// valid_until (which the legacy WebSocket handler never checked). The gRPC auth
// interceptor runs once per stream, so a token is looked up only when a device
// (re)connects — rare enough that a DB round-trip needs no caching.
type TokenStore struct {
	db  Querier
	now func() time.Time
}

func NewTokenStore(db Querier) *TokenStore {
	return &TokenStore{db: db, now: time.Now}
}

// Lookup returns the device id for a token, or one of the Err* sentinels.
func (s *TokenStore) Lookup(ctx context.Context, token string) (int64, error) {
	var (
		deviceID   int64
		validUntil time.Time
	)
	err := s.db.QueryRow(ctx, tokenQuery, token).Scan(&deviceID, &validUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrTokenUnknown
	}
	if err != nil {
		return 0, err
	}
	if !validUntil.After(s.now()) {
		return 0, ErrTokenExpired
	}
	return deviceID, nil
}
