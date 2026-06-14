package relay

import (
	"context"
	"log/slog"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/logger"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/outbox"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/pubsub"
	store "github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/spanner"
)

const sweepBatchLimit = 500

// Sweeper is the safety net behind the change-stream path. The stream consumer
// advances its watermark even when an individual publish fails, and entries
// written while the relay was down for longer than the bootstrap lookback are
// never seen by the stream at all. The sweeper re-scans PENDING rows older
// than minAge and publishes them, making the outbox delivery guarantee hold:
// once committed, an entry is eventually published.
type Sweeper struct {
	spannerClient *spanner.Client
	publisher     *pubsub.TopicPublisher
	interval      time.Duration
	minAge        time.Duration
	logger        *logger.Logger
}

func NewSweeper(
	spannerClient *spanner.Client,
	publisher *pubsub.TopicPublisher,
	interval, minAge time.Duration,
	log *logger.Logger,
) *Sweeper {
	return &Sweeper{
		spannerClient: spannerClient,
		publisher:     publisher,
		interval:      interval,
		minAge:        minAge,
		logger:        log,
	}
}

// Run sweeps on the configured interval until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweepOnce(ctx)
		}
	}
}

func (s *Sweeper) sweepOnce(ctx context.Context) {
	for shard := int64(0); shard < outbox.DefaultShardCount; shard++ {
		entries, err := store.ScanPendingEntries(ctx, s.spannerClient, shard, s.minAge, sweepBatchLimit)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Error(ctx, "outbox_sweep.scan_failed", "Failed to scan pending outbox entries",
				slog.Int64("shard", shard),
				slog.String("error", err.Error()),
			)
			continue
		}
		if len(entries) == 0 {
			continue
		}

		s.logger.Warn(ctx, "outbox_sweep.pending_found",
			"Found stale PENDING outbox entries missed by the change-stream path",
			slog.Int64("shard", shard),
			slog.Int("count", len(entries)),
		)

		var publishedIDs []string
		for _, e := range entries {
			env, data, err := parsePayload(e.Payload)
			if err != nil {
				s.logger.Error(ctx, "outbox_sweep.payload_parse_failed",
					"Failed to parse pending outbox payload; skipping entry",
					slog.String("entry_id", e.EntryID),
					slog.String("topic", e.Topic),
					slog.String("error", err.Error()),
				)
				continue
			}

			result := s.publisher.PublishFromOutbox(ctx, e.Topic, env, data, map[string]string{
				"video_id": e.VideoID,
			})
			if _, err := result.Get(ctx); err != nil {
				s.logger.Error(ctx, "outbox_sweep.publish_failed",
					"Failed to publish pending outbox entry; will retry next sweep",
					slog.String("entry_id", e.EntryID),
					slog.String("topic", e.Topic),
					slog.String("error", err.Error()),
				)
				continue
			}

			publishedIDs = append(publishedIDs, e.EntryID)
		}

		if len(publishedIDs) == 0 {
			continue
		}

		if err := store.MarkOutboxEntriesPublished(ctx, s.spannerClient, publishedIDs); err != nil {
			// At-least-once: the next sweep republishes and retries the mark.
			s.logger.Error(ctx, "outbox_sweep.mark_failed",
				"Failed to mark swept entries as published",
				slog.Int("count", len(publishedIDs)),
				slog.String("error", err.Error()),
			)
			continue
		}

		s.logger.Info(ctx, "outbox_sweep.published",
			"Republished stale PENDING outbox entries",
			slog.Int64("shard", shard),
			slog.Int("count", len(publishedIDs)),
		)
	}
}
