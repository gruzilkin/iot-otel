// Command iotd is the single IoT backend binary. Phase 1 serves the gRPC
// ingestion API; the HTTP/web tier and metrics are wired in later phases.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	ingestv1 "github.com/gruzilkin/iot-otel/api/gen/ingest/v1"
	"github.com/gruzilkin/iot-otel/internal/auth"
	"github.com/gruzilkin/iot-otel/internal/config"
	"github.com/gruzilkin/iot-otel/internal/ingest"
	"github.com/gruzilkin/iot-otel/internal/storage"
	"google.golang.org/grpc"
)

const (
	tokenCacheTTL    = 30 * time.Second
	gracefulStopWait = 5 * time.Second
	flushWait        = 10 * time.Second
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

	writer := storage.NewBatchWriter(pool, cfg.BatchMaxSize, cfg.BatchQueueCap, cfg.BatchMaxLatency, log)
	tokens := auth.NewTokenStore(pool, tokenCacheTTL)

	grpcServer := grpc.NewServer(grpc.ChainStreamInterceptor(auth.StreamAuthInterceptor(tokens)))
	ingestv1.RegisterIngestServiceServer(grpcServer, ingest.NewService(writer, log))

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("gRPC ingest listening", "addr", cfg.GRPCAddr)
		errCh <- grpcServer.Serve(lis)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-stop:
		log.Info("shutting down")
	}

	// Stop accepting and drain in-flight RPCs, but don't wait forever on a
	// long-lived device stream — force-stop after a grace window.
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

	flushCtx, cancel := context.WithTimeout(context.Background(), flushWait)
	defer cancel()
	return writer.Close(flushCtx)
}
