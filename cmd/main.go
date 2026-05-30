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
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/outbox"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/config"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/handler"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/poller"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/pubsub"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/spanner"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error: %v", err)
	}

}

func run() error {

	cfg, err := config.LoadOutboxPollerConfig()
	if err != nil {
		return err
	}

	logger := logger.New(cfg.Service(), cfg.Region(), cfg.AppEnv())

	logger.Info(
		context.Background(),
		"service.startup",
		"Upload API bootstrap started",
		slog.String("component", "bootstrap"),
		slog.String("http_addr", cfg.Port()),
		slog.String("project_id", cfg.ProjectID()),
		slog.String("spanner_database", cfg.SpannerDatabase()))

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

	logger.Info(
		ctx,
		"telemetry.initialized",
		"OpenTelemetry initialized",
		slog.String("component", "telemetry"),
		slog.String("project_id", cfg.ProjectID()),
	)

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(shutdownCtx); err != nil {
			logger.Error(
				context.Background(),
				"tracer.shutdown_failed",
				"Failed to shutdown tracer",
				slog.String("component", "telemetry"),
				slog.String("error", err.Error()),
				slog.Int("timeout_seconds", 5),
				slog.String("outcome", "failure"),
			)
			return
		}
		logger.Info(
			context.Background(),
			"tracer.shutdown_success",
			"Tracer shutdown completed",
			slog.String("component", "telemetry"),
			slog.Int("timeout_seconds", 5),
		)
	}()

	client, err := pubsub.NewClient(ctx, pubsub.Config{
		ProjectID:      cfg.ProjectID(),
		EnabledTracing: true,
	})

	if err != nil {
		return err
	}

	defer client.Close()

	logger.Info(
		ctx,
		"pubsub.client_initialized",
		"Pubsub client initialized",
		slog.String("component", "bootstrap"),
	)

	publisher := pubsub.NewTopicPublisher(client)
	defer publisher.Stop()

	spannerClient, err := spanner.NewClient(ctx, cfg.SpannerDatabase(), spanner.DefaultConfig())
	if err != nil {
		return err
	}
	defer spannerClient.Close()

	logger.Info(
		ctx,
		"spanner.client_initialized",
		"Spanner client initialized",
		slog.String("component", "bootstrap"),
		slog.String("spanner_database", cfg.SpannerDatabase()),
	)

	assignedShards := allShards(outbox.DefaultShardCount)
	poll := poller.NewPoller(spannerClient, publisher, cfg.BatchSize(), assignedShards, logger)
	logger.Info(
		ctx,
		"outbox_poller.initialized",
		"Outbox poller initialized",
		slog.String("component", "bootstrap"),
		slog.Int64("batch_size", cfg.BatchSize()),
		slog.Int("poll_interval_ms", cfg.PollIntervalMS()),
		slog.Int("assigned_shard_count", len(assignedShards)),
		slog.Any("assigned_shards", assignedShards),
	)

	logger.Info(
		ctx,
		"service.startup_complete",
		"All dependencies initialized, ready to receive messages",
		slog.String("component", "bootstrap"),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.Healthz)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	logger.Info(
		ctx,
		"server.configured",
		"HTTP server configured",
		slog.String("component", "http_server"),
		slog.String("addr", cfg.Port()),
		slog.String("routes", "GET /healthz,POST /upload"),
	)

	handlerErrCh := make(chan error, 1)
	serverErrCh := make(chan error, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info(
			ctx,
			"server.starting",
			"Starting HTTP server",
			slog.String("component", "http_server"),
			slog.String("addr", cfg.Port()),
		)
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				logger.Info(
					context.Background(),
					"server.stopped",
					"HTTP server stopped accepting new requests",
					slog.String("component", "http_server"),
					slog.String("addr", cfg.Port()),
				)
				return
			}
			serverErrCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info(ctx, "outbox_poller.starting", "Starting outbox poller",
			slog.String("component", "poller"),
			slog.Int64("batch_size", cfg.BatchSize()),
			slog.Int("poll_interval_ms", cfg.PollIntervalMS()),
			slog.Any("assigned_shards", assignedShards),
		)

		poll.Run(ctx, time.Duration(cfg.PollIntervalMS())*time.Millisecond)
	}()

	var runErr error

	select {
	case <-ctx.Done():
		logger.Info(
			ctx,
			"shutdown.initiated",
			"Shutdown signal received",
			slog.String("component", "lifecycle"),
			slog.String("reason", "signal"),
		)
	case err := <-serverErrCh:
		runErr = err
		logger.Error(
			ctx,
			"server.failed",
			"HTTP server failed",
			slog.String("component", "http_server"),
			slog.String("addr", cfg.Port()),
			slog.String("error", err.Error()),
			slog.String("outcome", "failure"),
		)
	case err := <-handlerErrCh:
		runErr = err
		logger.Error(
			ctx,
			"handler.failed",
			"Message handler failed",
			slog.String("component", "message_handler"),
			slog.String("error", err.Error()),
			slog.String("outcome", "failure"),
		)
	}
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error(
			ctx,
			"server.shutdown_failed",
			"Failed to shutdown HTTP server",
			slog.String("component", "http_server"),
			slog.String("error", err.Error()),
			slog.String("outcome", "failure"),
		)
		if runErr == nil {
			runErr = fmt.Errorf("http server shutdown: %w", err)
		}
	} else {
		logger.Info(
			ctx,
			"server.shutdown",
			"HTTP server shutdown complete",
			slog.String("component", "http_server"),
		)
	}

	wg.Wait()

	return runErr
}

func allShards(n int64) []int64 {
	shards := make([]int64, 0, n)
	for i := int64(0); i < n; i++ {
		shards = append(shards, i)
	}

	return shards
}
