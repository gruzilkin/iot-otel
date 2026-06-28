// Package testutil holds shared integration-test helpers. It is imported only by
// build-tagged (`integration`) test files, so although the package itself carries
// no build tag, its testcontainers/Docker machinery only ever runs in the
// integration build.
package testutil

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/gruzilkin/iot-otel/server/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// schemaFiles are applied, in order, to a fresh container. 5-dev-seed.sql is
// intentionally skipped: integration tests own their fixtures.
var schemaFiles = []string{
	"1-devices.sql",
	"2-device_access_tokens.sql",
	"3-sensor_data.sql",
	"4-sensor_data_weights.sql",
	"6-sessions.sql",
}

// Postgres is a throwaway Postgres container plus a connected pool.
type Postgres struct {
	Pool      *pgxpool.Pool
	container *postgres.PostgresContainer
}

// StartPostgres boots a disposable Postgres, applies the repo schema, and returns
// a ready pool. The caller owns the lifecycle and must call Close (typically from
// the package's TestMain).
func StartPostgres(ctx context.Context) (*Postgres, error) {
	scripts, err := schemaPaths()
	if err != nil {
		return nil, err
	}
	c, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithInitScripts(scripts...),
		postgres.WithDatabase("test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			// Postgres logs the ready line twice: once for the init-scripts boot,
			// once for the real start. Wait for the second.
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres container: %w", err)
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, fmt.Errorf("connection string: %w", err)
	}
	pool, err := storage.NewPool(ctx, dsn)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, fmt.Errorf("connect pool: %w", err)
	}
	return &Postgres{Pool: pool, container: c}, nil
}

// Close drains the pool and terminates the container.
func (p *Postgres) Close(ctx context.Context) error {
	p.Pool.Close()
	return p.container.Terminate(ctx)
}

// Truncate clears all data tables and resets identity sequences. Call it between
// tests so each starts from an empty, deterministic state.
func (p *Postgres) Truncate(ctx context.Context) error {
	_, err := p.Pool.Exec(ctx,
		`TRUNCATE devices, access_tokens, sensor_data, sensor_data_weights RESTART IDENTITY CASCADE`)
	return err
}

// schemaPaths resolves db/*.sql relative to this source file, so the working
// directory of the test run is irrelevant.
func schemaPaths() ([]string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("testutil: cannot resolve caller path")
	}
	// .../server/internal/testutil/postgres.go -> .../iot-otel/db
	dbDir := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "db")
	paths := make([]string, len(schemaFiles))
	for i, f := range schemaFiles {
		paths[i] = filepath.Join(dbDir, f)
	}
	return paths, nil
}
