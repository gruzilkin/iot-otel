// Package devices handles device and access-token management.
package devices

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrNotFound is returned when a device/token doesn't exist or isn't owned by
// the requesting user.
var ErrNotFound = errors.New("devices: not found")

type Device struct {
	ID     int64
	UserID int64
	Name   string
}

type Token struct {
	Token      string
	DeviceID   int64
	CreatedAt  time.Time
	ValidUntil time.Time
}

// DB is the subset of pgxpool.Pool the repo needs.
type DB interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

type Repo struct{ db DB }

func NewRepo(db DB) *Repo { return &Repo{db: db} }

func (r *Repo) FindAllByUserID(ctx context.Context, userID int64) ([]Device, error) {
	rows, err := r.db.Query(ctx, `SELECT device_id, user_id, name FROM devices WHERE user_id = $1 ORDER BY device_id`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.ID, &d.UserID, &d.Name); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (r *Repo) FindByIDAndUserID(ctx context.Context, id, userID int64) (Device, error) {
	var d Device
	err := r.db.QueryRow(ctx, `SELECT device_id, user_id, name FROM devices WHERE device_id = $1 AND user_id = $2`, id, userID).
		Scan(&d.ID, &d.UserID, &d.Name)
	if errors.Is(err, pgx.ErrNoRows) {
		return Device{}, ErrNotFound
	}
	return d, err
}

func (r *Repo) Insert(ctx context.Context, userID int64, name string) (Device, error) {
	var d Device
	err := r.db.QueryRow(ctx, `INSERT INTO devices (user_id, name) VALUES ($1, $2) RETURNING device_id, user_id, name`, userID, name).
		Scan(&d.ID, &d.UserID, &d.Name)
	return d, err
}

func (r *Repo) DeleteByIDAndUserID(ctx context.Context, id, userID int64) error {
	tag, err := r.db.Exec(ctx, `DELETE FROM devices WHERE device_id = $1 AND user_id = $2`, id, userID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repo) ListTokens(ctx context.Context, deviceID int64) ([]Token, error) {
	rows, err := r.db.Query(ctx, `SELECT token, device_id, created_at, valid_until FROM access_tokens WHERE device_id = $1 ORDER BY created_at`, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Token
	for rows.Next() {
		var t Token
		if err := rows.Scan(&t.Token, &t.DeviceID, &t.CreatedAt, &t.ValidUntil); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (r *Repo) InsertToken(ctx context.Context, t Token) error {
	_, err := r.db.Exec(ctx, `INSERT INTO access_tokens (token, device_id, created_at, valid_until) VALUES ($1, $2, $3, $4)`,
		t.Token, t.DeviceID, t.CreatedAt, t.ValidUntil)
	return err
}

func (r *Repo) DeleteToken(ctx context.Context, token string, deviceID int64) error {
	tag, err := r.db.Exec(ctx, `DELETE FROM access_tokens WHERE token = $1 AND device_id = $2`, token, deviceID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
