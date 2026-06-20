// Command iotd is the single IoT backend binary: it serves the gRPC ingestion
// API (devices) and the HTTP/WebSocket API (browsers), bridged by an in-memory
// hub. The web UI, auth, and metrics arrive in later phases.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	ingestv1 "github.com/gruzilkin/iot-otel/api/gen/ingest/v1"
	"github.com/gruzilkin/iot-otel/internal/auth"
	"github.com/gruzilkin/iot-otel/internal/charts"
	"github.com/gruzilkin/iot-otel/internal/config"
	"github.com/gruzilkin/iot-otel/internal/devices"
	"github.com/gruzilkin/iot-otel/internal/hub"
	"github.com/gruzilkin/iot-otel/internal/ingest"
	"github.com/gruzilkin/iot-otel/internal/realtime"
	"github.com/gruzilkin/iot-otel/internal/sensors"
	"github.com/gruzilkin/iot-otel/internal/storage"
	"github.com/gruzilkin/iot-otel/internal/web"
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
	tokenCacheTTL    = 30 * time.Second
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
	tokens := auth.NewTokenStore(pool, tokenCacheTTL)

	grpcServer := grpc.NewServer(grpc.ChainStreamInterceptor(auth.StreamAuthInterceptor(tokens)))
	ingestv1.RegisterIngestServiceServer(grpcServer, ingest.NewService(writer, h, log))

	sessions := auth.NewSessionManager(pool, cfg.CookieSecure)
	var provider auth.Provider
	if cfg.OAuthClientID != "" {
		provider = auth.NewGitHubProvider(cfg.OAuthClientID, cfg.OAuthClientSecret, cfg.OAuthRedirectURL)
	}
	authH := auth.New(sessions, provider, log)

	deviceSvc := devices.NewService(devices.NewRepo(pool))
	authz := authorizer{auth: authH, devices: deviceSvc}

	chartsHandler := charts.NewHandler(sensors.NewService(sensors.NewPgxRepo(pool)), authz, log)
	realtimeHandler := realtime.NewHandler(h, authz, cfg.WSAllowedOrigins, log)
	devicesHandler := devices.NewHandler(deviceSvc, authH, authH.CSRFToken, log)

	mux := http.NewServeMux()

	// Auth endpoints (no session required).
	mux.HandleFunc("GET /login", authH.Login)
	mux.HandleFunc("GET /oauth2/callback", authH.Callback)
	mux.HandleFunc("POST /logout", authH.Logout)
	if cfg.DevLoginUserID > 0 {
		log.Warn("dev login enabled", "user_id", cfg.DevLoginUserID)
		mux.Handle("GET /dev-login", authH.DevLogin(cfg.DevLoginUserID))
	}

	// Static assets (public).
	mux.Handle("GET /css/", web.StaticHandler())

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
