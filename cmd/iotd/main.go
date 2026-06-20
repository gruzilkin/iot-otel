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
	"github.com/gruzilkin/iot-otel/internal/config"
	"github.com/gruzilkin/iot-otel/internal/hub"
	"github.com/gruzilkin/iot-otel/internal/ingest"
	"github.com/gruzilkin/iot-otel/internal/realtime"
	"github.com/gruzilkin/iot-otel/internal/storage"
	"google.golang.org/grpc"
)

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

	mux := http.NewServeMux()
	mux.Handle("GET /charts/{deviceId}/realtime", realtime.NewHandler(h, cfg.WSAllowedOrigins, log))
	mux.HandleFunc("GET /charts/{deviceId}", livePage)
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}

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

// livePage is a minimal debugging page that streams a device's readings over
// the realtime WebSocket. The full ECharts UI replaces it in a later phase.
func livePage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(livePageHTML))
}

const livePageHTML = `<!doctype html><meta charset=utf-8><title>live</title>
<h3>live readings</h3><pre id=out>connecting…</pre>
<script>
const out = document.getElementById('out');
const proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
const ws = new WebSocket(proto + location.host + location.pathname + '/realtime');
ws.onmessage = e => {
  if (e.data === 'pong') return;
  out.textContent = new Date().toISOString() + '  ' + e.data + '\n' + out.textContent;
};
ws.onclose = () => out.textContent = 'closed\n' + out.textContent;
setInterval(() => ws.readyState === 1 && ws.send('ping'), 30000);
</script>`
