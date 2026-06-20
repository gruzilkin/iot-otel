-- Server-side session store for the web tier (alexedwards/scs pgxstore).
-- Additive: does not touch the reused sensor/device tables.
CREATE TABLE sessions (
    token TEXT PRIMARY KEY,
    data BYTEA NOT NULL,
    expiry TIMESTAMPTZ NOT NULL
);
CREATE INDEX sessions_expiry_idx ON sessions (expiry);
