// Package storage owns persistence of sensor readings to PostgreSQL.
package storage

import (
	"context"

	"github.com/gruzilkin/iot-otel/internal/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Writer accepts readings for durable storage. Implementations may batch.
type Writer interface {
	// Enqueue hands a reading off for writing. It may block to apply
	// backpressure, and returns ErrClosed once the writer is shutting down.
	Enqueue(r model.Reading) error
	// Close flushes buffered readings and stops the writer.
	Close(ctx context.Context) error
}

// DB is the subset of pgxpool.Pool the writer needs, kept small for testing.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// NewPool opens a pgx connection pool.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, dsn)
}
