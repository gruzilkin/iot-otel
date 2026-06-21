package auth

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type fakeRow struct {
	deviceID   int64
	validUntil time.Time
	err        error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*int64)) = r.deviceID
	*(dest[1].(*time.Time)) = r.validUntil
	return nil
}

type fakeQuerier struct {
	row fakeRow
}

func (q *fakeQuerier) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return q.row
}

var fixedNow = time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)

func newStore(q Querier) *TokenStore {
	s := NewTokenStore(q)
	s.now = func() time.Time { return fixedNow }
	return s
}

func TestLookupValid(t *testing.T) {
	q := &fakeQuerier{row: fakeRow{deviceID: 42, validUntil: fixedNow.Add(time.Hour)}}
	id, err := newStore(q).Lookup(context.Background(), "tok")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if id != 42 {
		t.Fatalf("want device 42, got %d", id)
	}
}

func TestLookupExpired(t *testing.T) {
	q := &fakeQuerier{row: fakeRow{deviceID: 42, validUntil: fixedNow.Add(-time.Second)}}
	if _, err := newStore(q).Lookup(context.Background(), "tok"); err != ErrTokenExpired {
		t.Fatalf("want ErrTokenExpired, got %v", err)
	}
}

func TestLookupUnknown(t *testing.T) {
	q := &fakeQuerier{row: fakeRow{err: pgx.ErrNoRows}}
	if _, err := newStore(q).Lookup(context.Background(), "tok"); err != ErrTokenUnknown {
		t.Fatalf("want ErrTokenUnknown, got %v", err)
	}
}
