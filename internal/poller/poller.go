package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pb "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/spanner"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/logger"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/outbox"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/pubsub"
	store "github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/spanner"
)

type Poller struct {
	spanner        *spanner.Client
	publisher      *pubsub.TopicPublisher
	batchSize      int64
	assignedShards []int64
	logger         *logger.Logger
}

type pendingPublish struct {
	entryID string
	videoID string
	result  *pb.PublishResult
}

func NewPoller(spannerClient *spanner.Client, publisher *pubsub.TopicPublisher, batchSize int64, assignedShards []int64, logger *logger.Logger) *Poller {

	shards := append([]int64(nil), assignedShards...)
	return &Poller{
		spanner:        spannerClient,
		publisher:      publisher,
		batchSize:      batchSize,
		assignedShards: shards,
		logger:         logger,
	}
}

func (p *Poller) Run(ctx context.Context, interval time.Duration) {

	if interval <= 0 {
		interval = time.Second
	}

	p.logger.Info(
		ctx,
		"outbox.poller_loop_started",
		"Started outbox poll loop",
		slog.Duration("interval", interval),
		slog.Int64("batch_size", p.batchSize),
		slog.Int("assigned_shard_count", len(p.assignedShards)),
		slog.Any("assigned_shards", p.assignedShards),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var (
		running atomic.Bool
		wg      sync.WaitGroup
	)
	defer wg.Wait()

	runTick := func() {
		if !running.CompareAndSwap(false, true) {
			p.logger.Warn(
				ctx,
				"outbox.tick_skipped",
				"Skipped outbox poll tick because the previous tick is still running",
				slog.Int("assigned_shard_count", len(p.assignedShards)),
			)
			return
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer running.Store(false)

			if err := p.Poll(ctx); err != nil && ctx.Err() == nil {
				p.logger.Error(ctx, "outbox.poll_failed", "Outbox poll failed", slog.String("error", err.Error()))
			}
		}()
	}

	runTick()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info(ctx, "outbox.poller_loop_stopped", "Stopped outbox poll loop", slog.String("reason", "context_canceled"))
			return
		case <-ticker.C:
			runTick()
		}
	}

}

func (p *Poller) Poll(ctx context.Context) error {

	p.logger.Info(
		ctx,
		"outbox.poll_started",
		"Started outbox poll cycle",
		slog.Int64("batch_size", p.batchSize),
		slog.Int("assigned_shard_count", len(p.assignedShards)),
		slog.Any("assigned_shards", p.assignedShards),
	)

	totalEntries := 0
	totalPublished := 0
	totalTopics := 0

	defer func() {

		p.logger.Info(
			ctx,
			"outbox.poll_completed",
			"Completed outbox poll cycle",
			slog.Int("entries_read", totalEntries),
			slog.Int("entries_published", totalPublished),
			slog.Int("topic_group_count", totalTopics),
		)
	}()

	for _, shardID := range p.assignedShards {

		shardStart := time.Now()
		rows, err := store.ReadPendingOutboxEntries(ctx, p.spanner, shardID, p.batchSize)
		if err != nil {

			p.logger.Error(
				ctx,
				"outbox.shard_read_failed",
				"Failed to read pending outbox entries for shard",
				slog.Int64("shard_id", shardID),
				slog.String("error", err.Error()),
			)
			return fmt.Errorf("read pending outbox entries for shard %d: %w", shardID, err)
		}

		totalEntries += len(rows)

		if len(rows) == 0 {
			p.logger.Debug(
				ctx,
				"outbox.shard_empty",
				"No pending outbox entries for shard",
				slog.Int64("shard_id", shardID),
			)
			continue
		}

		grouped := groupEntriesByTopic(rows)
		totalTopics += len(grouped)

		p.logger.Info(
			ctx,
			"outbox.shard_loaded",
			"Loaded pending outbox entries for shard",
			slog.Int64("shard_id", shardID),
			slog.Int("entry_count", len(rows)),
			slog.Int("topic_group_count", len(grouped)),
		)

		shardPublished := 0

		for topic, entries := range grouped {
			publishedCount, err := p.publishTopicGroup(ctx, shardID, topic, entries)
			if err != nil {
				return err
			}
			shardPublished += publishedCount
			totalPublished += publishedCount
		}

		p.logger.Info(
			ctx,
			"outbox.shard_completed",
			"Completed outbox shard poll",
			slog.Int64("shard_id", shardID),
			slog.Duration("duration", time.Since(shardStart)),
			slog.Int("entry_count", len(rows)),
			slog.Int("published_count", shardPublished),
		)

	}
	return nil
}

func (p *Poller) publishTopicGroup(ctx context.Context, shardID int64, topic string, entries []store.PendingOutboxEntry) (int, error) {
	p.logger.Info(
		ctx,
		"outbox.publish_started",
		"Started publishing outbox topic batch",
		slog.Int64("shard_id", shardID),
		slog.String("topic", topic),
		slog.Int("entry_count", len(entries)),
	)

	publishes := make([]pendingPublish, 0, len(entries))

	for _, entry := range entries {
		env, data, err := parsePayload(entry.Payload)
		if err != nil {
			p.logger.Error(
				ctx,
				"outbox.payload_parse_failed",
				"Failed to parse outbox payload",
				slog.Int64("shard_id", shardID),
				slog.String("topic", topic),
				slog.String("entry_id", entry.EntryID),
				slog.String("video_id", entry.VideoID),
				slog.String("error", err.Error()),
			)
			continue
		}

		result := p.publisher.PublishFromOutbox(ctx, topic, env, data, map[string]string{
			"video_id": entry.VideoID,
		})

		publishes = append(publishes, pendingPublish{
			entryID: entry.EntryID,
			videoID: entry.VideoID,
			result:  result,
		})
	}

	publishedEntryIDs := make([]string, 0, len(publishes))
	for _, publish := range publishes {
		_, err := publish.result.Get(ctx)
		if err != nil {

			p.logger.Error(ctx,
				"outbox.publish_failed",
				"Failed to publish outbox entry",
				slog.Int64("shard_id", shardID),
				slog.String("topic", topic),
				slog.String("entry_id", publish.entryID),
				slog.String("video_id", publish.videoID),
				slog.String("error", err.Error()),
			)
			continue

		}

		publishedEntryIDs = append(publishedEntryIDs, publish.entryID)
	}

	if len(publishedEntryIDs) == 0 {
		p.logger.Warn(
			ctx,
			"outbox.publish_no_successes",
			"Finished outbox topic batch without successful publishes",
			slog.Int64("shard_id", shardID),
			slog.String("topic", topic),
			slog.Int("entry_count", len(entries)),
		)
		return 0, nil
	}

	if err := store.MarkOutboxEntriesPublished(ctx, p.spanner, publishedEntryIDs); err != nil {

		p.logger.Error(
			ctx,
			"outbox.mark_published_failed",
			"Failed to mark outbox entries as published",
			slog.Int64("shard_id", shardID),
			slog.String("topic", topic),
			slog.Int("count", len(publishedEntryIDs)),
			slog.String("error", err.Error()),
		)

		return 0, fmt.Errorf("mark published entries for shard %d topic %q: %w", shardID, topic, err)
	}

	p.logger.Info(ctx,
		"outbox.publish_success",
		"Published outbox batch",
		slog.Int64("shard_id", shardID),
		slog.String("topic", topic),
		slog.Int("count", len(publishedEntryIDs)),
	)

	return len(publishedEntryIDs), nil

}

func parsePayload(payload spanner.NullJSON) (outbox.Envelope, []byte, error) {
	if !payload.Valid {
		return outbox.Envelope{}, nil, fmt.Errorf("payload is null")
	}

	raw, err := json.Marshal(payload.Value)
	if err != nil {
		return outbox.Envelope{}, nil, fmt.Errorf("marshal JSON payload: %w", err)
	}

	env, err := outbox.ParseEnvelope(raw)
	if err != nil {
		return outbox.Envelope{}, nil, err
	}

	data, err := json.Marshal(env.Data)
	if err != nil {
		return outbox.Envelope{}, nil, fmt.Errorf("marshal envelope data: %w", err)
	}

	return env, data, nil
}

func groupEntriesByTopic(entries []store.PendingOutboxEntry) map[string][]store.PendingOutboxEntry {
	grouped := make(map[string][]store.PendingOutboxEntry)
	for _, entry := range entries {
		grouped[entry.Topic] = append(grouped[entry.Topic], entry)
	}

	return grouped
}
