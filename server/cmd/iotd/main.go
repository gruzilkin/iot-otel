// Command iotd is the single IoT backend binary: it serves the gRPC ingestion
// API (devices) and the HTTP API (browsers, incl. an SSE realtime stream), bridged by an in-memory
// hub. The web UI, auth, and metrics arrive in later phases.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	ingestv1 "github.com/gruzilkin/iot-otel/server/api/gen/ingest/v1"
	"github.com/gruzilkin/iot-otel/server/internal/auth"
	"github.com/gruzilkin/iot-otel/server/internal/charts"
	"github.com/gruzilkin/iot-otel/server/internal/config"
	"github.com/gruzilkin/iot-otel/server/internal/devices"
	"github.com/gruzilkin/iot-otel/server/internal/hub"
	"github.com/gruzilkin/iot-otel/server/internal/ingest"
	"github.com/gruzilkin/iot-otel/server/internal/metrics"
	"github.com/gruzilkin/iot-otel/server/internal/realtime"
	"github.com/gruzilkin/iot-otel/server/internal/sensors"
	"github.com/gruzilkin/iot-otel/server/internal/storage"
	"github.com/gruzilkin/iot-otel/server/internal/web"
	"google.golang.org/grpc"
)

// authorizer adapts the auth session + device ownership into the single-method
// Authorizer the charts/realtime handlers expect.
type authorizer struct {
	auth    *auth.Auth
	devices *devices.Service
}

func (a authorizer) Authorize(ctx context.Context, deviceID int64) (bool, error) {
	uid, ok := a.auth.UserID(ctx)
	if !ok {
		return false, nil
	}
	return a.devices.CanAccess(ctx, uid, deviceID)
}

const (
	gracefulStopWait = 5 * time.Second
	shutdownWait     = 10 * time.Second
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg := config.Load()

	pool, err := storage.NewPool(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	h := hub.New()
	writer := storage.NewBatchWriter(pool, cfg.BatchMaxSize, cfg.BatchQueueCap, cfg.BatchMaxLatency, log)
	tokens := auth.NewTokenStore(pool)

	if metrics.Enabled() {
		mp, err := metrics.NewMeterProvider(context.Background())
		if err != nil {
			return err
		}
		defer func() { _ = mp.Shutdown(context.Background()) }()
		meter := mp.Meter("github.com/gruzilkin/iot-otel/server")
		agg, err := metrics.NewAggregator(meter)
		if err != nil {
			return err
		}
		go agg.Run(h.SubscribeAll())
		if err := metrics.RegisterRuntime(meter, h, writer); err != nil {
			return err
		}
		log.Info("OTel metrics enabled")
	} else {
		log.Info("OTel metrics disabled (set OTEL_EXPORTER_OTLP_ENDPOINT to enable)")
	}

	// gRPC is served in plaintext; a fronting proxy (e.g. nginx) terminates TLS
	// and forwards to this service on a trusted network.
	grpcServer := grpc.NewServer(grpc.ChainStreamInterceptor(auth.StreamAuthInterceptor(tokens)))
	ingestv1.RegisterIngestServiceServer(grpcServer, ingest.NewService(writer, h, log))

	sessions := auth.NewSessionManager()
	// GitHub OAuth is the only login path, so it is required: without it the web
	// tier has no way to authenticate. Fail fast rather than booting a server
	// that silently 501s every login attempt.
	if cfg.OAuthClientID == "" || cfg.OAuthClientSecret == "" {
		return fmt.Errorf("OAUTH_GITHUB_CLIENT_ID and OAUTH_GITHUB_CLIENT_SECRET are required")
	}
	provider := auth.NewGitHubProvider(cfg.OAuthClientID, cfg.OAuthClientSecret, cfg.OAuthRedirectURL)
	authH := auth.New(sessions, provider, log)

	deviceSvc := devices.NewService(devices.NewRepo(pool))
	authz := authorizer{auth: authH, devices: deviceSvc}

	chartsHandler := charts.NewHandler(sensors.NewService(sensors.NewPgxRepo(pool)), authz, log)
	realtimeHandler := realtime.NewHandler(h, authz, log)
	devicesHandler := devices.NewHandler(deviceSvc, authH, authH.CSRFToken, log)

	mux := http.NewServeMux()

	// Auth endpoints (no session required).
	mux.HandleFunc("GET /login", authH.Login)
	mux.HandleFunc("GET /oauth2/callback", authH.Callback)
	mux.HandleFunc("POST /logout", authH.Logout)

	// Static assets (public).
	mux.Handle("GET /css/", web.StaticHandler())

	// Health/readiness (public).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			http.Error(w, "db unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready"))
	})

	// Charts: page redirects to login when unauthenticated; partial + realtime
	// return 401 (matching the legacy entry-point behaviour).
	mux.Handle("GET /charts/{deviceId}/realtime", authH.RequireUserAPI(realtimeHandler))
	mux.Handle("GET /charts/{deviceId}/partial", authH.RequireUserAPI(http.HandlerFunc(chartsHandler.Partial)))
	mux.Handle("GET /charts/{deviceId}", authH.RequireUser(http.HandlerFunc(chartsHandler.Page)))

	// Device management (HTML/HTMX; redirect on missing session).
	mux.Handle("GET /devices", authH.RequireUser(http.HandlerFunc(devicesHandler.Index)))
	mux.Handle("POST /devices", authH.RequireUser(http.HandlerFunc(devicesHandler.Create)))
	mux.Handle("GET /devices/{id}", authH.RequireUser(http.HandlerFunc(devicesHandler.Detail)))
	mux.Handle("DELETE /devices/{id}", authH.RequireUser(http.HandlerFunc(devicesHandler.Delete)))
	mux.Handle("POST /devices/{id}/tokens", authH.RequireUser(http.HandlerFunc(devicesHandler.AddToken)))
	mux.Handle("DELETE /devices/{deviceId}/tokens/{tokenId}", authH.RequireUser(http.HandlerFunc(devicesHandler.DeleteToken)))

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/devices", http.StatusFound)
	})

	// Sessions load/save wraps everything; CSRF guards mutating requests.
	rootHandler := sessions.LoadAndSave(authH.RequireCSRF(mux))
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: rootHandler}

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}

	errCh := make(chan error, 2)
	go func() {
		log.Info("gRPC ingest listening", "addr", cfg.GRPCAddr)
		errCh <- grpcServer.Serve(grpcLis)
	}()
	go func() {
		log.Info("HTTP listening", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-stop:
		log.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownWait)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(gracefulStopWait):
		log.Warn("graceful stop timed out; forcing")
		grpcServer.Stop()
	}

	return writer.Close(shutdownCtx)
}
