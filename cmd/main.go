package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"sync"

	"os/signal"
	"syscall"
	"time"

	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/logger"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/otel"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/config"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/handler"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/pubsub"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/relay"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/spanner"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func run() error {

	cfg, err := config.LoadOutboxRelayConfig()
	if err != nil {
		return err
	}

	log := logger.New(cfg.Service(), cfg.Region(), cfg.AppEnv())

	log.Info(
		context.Background(),
		"service.startup",
		"Outbox relay bootstrap started",
		slog.String("component", "bootstrap"),
		slog.String("http_addr", cfg.Port()),
		slog.String("project_id", cfg.ProjectID()),
		slog.String("spanner_database", cfg.SpannerDatabase()),
		slog.String("change_stream_name", cfg.ChangeStreamName()),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	trCfg := otel.TracerConfig{
		AppEnv:      cfg.AppEnv(),
		ServiceName: cfg.Service(),
		ProjectID:   cfg.ProjectID(),
		Region:      cfg.Region(),
	}

	shutdown, err := otel.InitTracer(ctx, trCfg)
	if err != nil {
		return err
	}

	log.Info(ctx, "telemetry.initialized", "OpenTelemetry initialized",
		slog.String("component", "telemetry"),
		slog.String("project_id", cfg.ProjectID()),
	)

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(shutdownCtx); err != nil {
			log.Error(context.Background(), "tracer.shutdown_failed", "Failed to shutdown tracer",
				slog.String("component", "telemetry"),
				slog.String("error", err.Error()),
			)
			return
		}
		log.Info(context.Background(), "tracer.shutdown_success", "Tracer shutdown completed",
			slog.String("component", "telemetry"),
		)
	}()

	pubsubClient, err := pubsub.NewClient(ctx, pubsub.Config{
		ProjectID:      cfg.ProjectID(),
		EnabledTracing: true,
	})
	if err != nil {
		return err
	}
	defer pubsubClient.Close()

	log.Info(ctx, "pubsub.client_initialized", "Pub/Sub client initialized",
		slog.String("component", "bootstrap"),
	)

	publisher := pubsub.NewTopicPublisher(pubsubClient)
	defer publisher.Stop()

	spannerClient, err := spanner.NewClient(ctx, cfg.SpannerDatabase(), spanner.DefaultConfig())
	if err != nil {
		return err
	}
	defer spannerClient.Close()

	log.Info(ctx, "spanner.client_initialized", "Spanner client initialized",
		slog.String("component", "bootstrap"),
		slog.String("spanner_database", cfg.SpannerDatabase()),
	)

	r := relay.New(
		spannerClient,
		publisher,
		cfg.ChangeStreamName(),
		cfg.HeartbeatIntervalMS(),
		cfg.StartLookbackSecs(),
		log,
	)

	log.Info(ctx, "outbox_relay.initialized", "Outbox relay initialized",
		slog.String("component", "bootstrap"),
		slog.String("change_stream_name", cfg.ChangeStreamName()),
		slog.Int("heartbeat_interval_ms", cfg.HeartbeatIntervalMS()),
		slog.Int("start_lookback_seconds", cfg.StartLookbackSecs()),
	)

	sweeper := relay.NewSweeper(
		spannerClient,
		publisher,
		time.Duration(cfg.SweepIntervalSecs())*time.Second,
		time.Duration(cfg.SweepMinAgeSecs())*time.Second,
		log,
	)

	cleanupHandler := handler.NewCleanupHandler(log, spannerClient,
		time.Duration(cfg.PublishedRetentionHrs())*time.Hour)

	log.Info(ctx, "service.startup_complete", "All dependencies initialized, ready to relay",
		slog.String("component", "bootstrap"),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.Healthz)
	mux.Handle("POST /cleanup", cleanupHandler)

	server := &http.Server{
		Addr:    cfg.Port(),
		Handler: mux,
	}

	serverErrCh := make(chan error, 1)
	relayErrCh := make(chan error, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info(ctx, "server.starting", "Starting HTTP server",
			slog.String("component", "http_server"),
			slog.String("addr", cfg.Port()),
		)
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				log.Info(context.Background(), "server.stopped", "HTTP server stopped",
					slog.String("component", "http_server"),
				)
				return
			}
			serverErrCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info(ctx, "outbox_relay.starting", "Starting outbox relay",
			slog.String("component", "relay"),
			slog.String("change_stream_name", cfg.ChangeStreamName()),
		)
		if err := r.Run(ctx); err != nil {
			relayErrCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info(ctx, "outbox_sweep.starting", "Starting PENDING sweep fallback",
			slog.String("component", "sweeper"),
			slog.Int("interval_seconds", cfg.SweepIntervalSecs()),
			slog.Int("min_age_seconds", cfg.SweepMinAgeSecs()),
		)
		sweeper.Run(ctx)
	}()

	var runErr error

	select {
	case <-ctx.Done():
		log.Info(ctx, "shutdown.initiated", "Shutdown signal received",
			slog.String("component", "lifecycle"),
			slog.String("reason", "signal"),
		)
	case err := <-serverErrCh:
		runErr = err
		log.Error(ctx, "server.failed", "HTTP server failed",
			slog.String("component", "http_server"),
			slog.String("error", err.Error()),
		)
	case err := <-relayErrCh:
		runErr = err
		log.Error(ctx, "outbox_relay.failed", "Outbox relay failed",
			slog.String("component", "relay"),
			slog.String("error", err.Error()),
		)
	}
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error(ctx, "server.shutdown_failed", "Failed to shutdown HTTP server",
			slog.String("component", "http_server"),
			slog.String("error", err.Error()),
		)
		if runErr == nil {
			runErr = fmt.Errorf("http server shutdown: %w", err)
		}
	} else {
		log.Info(ctx, "server.shutdown", "HTTP server shutdown complete",
			slog.String("component", "http_server"),
		)
	}

	wg.Wait()

	return runErr
}
