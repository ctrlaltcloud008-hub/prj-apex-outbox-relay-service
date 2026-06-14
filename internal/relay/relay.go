package relay

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/logger"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/checkpoint"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/pubsub"
)

// Relay consumes a Spanner Change Stream and publishes outbox entries to Pub/Sub.
type Relay struct {
	spannerClient    *spanner.Client
	publisher        *pubsub.TopicPublisher
	streamName       string
	heartbeatMS      int
	startLookbackSec int
	logger           *logger.Logger
}

func New(
	spannerClient *spanner.Client,
	publisher *pubsub.TopicPublisher,
	streamName string,
	heartbeatMS int,
	startLookbackSec int,
	log *logger.Logger,
) *Relay {
	return &Relay{
		spannerClient:    spannerClient,
		publisher:        publisher,
		streamName:       streamName,
		heartbeatMS:      heartbeatMS,
		startLookbackSec: startLookbackSec,
		logger:           log,
	}
}

// Run starts the relay. It reads existing checkpoints and resumes each active
// partition, or bootstraps from the initial partition if none exist.
// Blocks until ctx is cancelled or a fatal error occurs.
func (r *Relay) Run(ctx context.Context) error {
	checkpoints, err := checkpoint.ReadActiveCheckpoints(ctx, r.spannerClient)
	if err != nil {
		return fmt.Errorf("read active checkpoints: %w", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	var spawn func(token string, startTs time.Time)
	spawn = func(token string, startTs time.Time) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.consumePartition(ctx, spawn, token, startTs); err != nil && ctx.Err() == nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()
	}

	if len(checkpoints) == 0 {
		// No prior checkpoints — bootstrap with the initial (nil-token) query.
		// consumePartition with an empty token runs the initial partition enumeration
		// and immediately returns after spawning goroutines for the real partitions.
		r.logger.Info(ctx, "outbox_relay.bootstrap",
			"No active checkpoints found; bootstrapping from initial partition",
			slog.Int("lookback_seconds", r.startLookbackSec),
		)
		spawn("", time.Now().UTC().Add(-time.Duration(r.startLookbackSec)*time.Second))
	} else {
		r.logger.Info(ctx, "outbox_relay.resume",
			"Resuming from existing checkpoints",
			slog.Int("partition_count", len(checkpoints)),
		)
		for _, cp := range checkpoints {
			spawn(cp.PartitionToken, cp.Watermark)
		}
	}

	// Wait for all partitions to finish or for a fatal error / context cancel.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		wg.Wait()
		return nil
	case err := <-errCh:
		wg.Wait()
		return err
	case <-done:
		return nil
	}
}
