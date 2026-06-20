// Package config loads runtime configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GRPCAddr         string
	HTTPAddr         string
	DatabaseURL      string
	BatchMaxSize     int
	BatchMaxLatency  time.Duration
	BatchQueueCap    int
	WSAllowedOrigins []string

	OAuthClientID     string
	OAuthClientSecret string
	OAuthRedirectURL  string
	CookieSecure      bool
	DevLoginUserID    int64
}

func Load() Config {
	return Config{
		GRPCAddr:         env("GRPC_ADDR", ":50051"),
		HTTPAddr:         env("HTTP_ADDR", ":8080"),
		DatabaseURL:      databaseURL(),
		BatchMaxSize:     envInt("BATCH_MAX_SIZE", 500),
		BatchMaxLatency:  envDuration("BATCH_MAX_LATENCY", 500*time.Millisecond),
		BatchQueueCap:    envInt("BATCH_QUEUE_CAP", 4096),
		WSAllowedOrigins: envList("WS_ALLOWED_ORIGINS"),

		OAuthClientID:     os.Getenv("OAUTH_GITHUB_CLIENT_ID"),
		OAuthClientSecret: os.Getenv("OAUTH_GITHUB_CLIENT_SECRET"),
		OAuthRedirectURL:  env("OAUTH_REDIRECT_URL", "http://localhost:8080/oauth2/callback"),
		CookieSecure:      envBool("COOKIE_SECURE", false),
		DevLoginUserID:    int64(envInt("DEV_LOGIN_USER_ID", 0)),
	}
}

// databaseURL prefers an explicit DATABASE_URL, otherwise assembles one from the
// DB_* variables the existing docker-compose stack already uses (same defaults).
func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		env("DB_USER", "user"),
		env("DB_PASSWORD", "secret"),
		env("DB_HOST", "localhost"),
		env("DB_PORT", "5432"),
		env("DB_NAME", "fileserver"),
	)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// envList splits a comma-separated env var; empty yields nil (same-origin only
// for WebSocket origin checks).
func envList(key string) []string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
