package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/outbox"
	"github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/checkpoint"
	store "github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

const outboxTable = "outbox"

// consumePartition consumes one Change Stream partition, clamping the bootstrap
// start timestamp forward if it predates the stream's earliest readable point.
//
// On bootstrap the configured lookback can place start_ts before the change
// stream's earliest readable point — most often during the first lookback
// window after the stream is created, since a stream cannot be read from before
// it existed. Spanner rejects that read with OutOfRange. Nothing exists to read
// before the stream began, so we clamp the start to now and retry once rather
// than letting the error propagate up and crash the relay; the PENDING sweep
// backstops any entry committed in the clamped-away gap.
func (r *Relay) consumePartition(ctx context.Context, spawn func(string, time.Time), token string, startTs time.Time) error {
	isInitial := token == ""

	logToken := token
	if isInitial {
		logToken = "<initial>"
	}

	clamped := false
	for {
		err := r.readPartition(ctx, spawn, token, logToken, isInitial, startTs)
		if isInitial && !clamped && spanner.ErrCode(err) == codes.OutOfRange {
			now := time.Now().UTC()
			r.logger.Warn(ctx, "outbox_relay.start_clamped",
				"Bootstrap start_timestamp predates the change stream; clamping to now and retrying",
				slog.Time("requested_start_ts", startTs),
				slog.Time("clamped_start_ts", now),
				slog.String("error", err.Error()),
			)
			startTs = now
			clamped = true
			continue
		}
		return err
	}
}

// readPartition runs the Change Stream query for one partition and processes
// all records until the partition ends (ChildPartitionsRecord received) or ctx
// is cancelled. spawn is called to start goroutines for child partitions.
func (r *Relay) readPartition(ctx context.Context, spawn func(string, time.Time), token, logToken string, isInitial bool, startTs time.Time) error {
	r.logger.Info(ctx, "outbox_relay.partition_started",
		"Change Stream partition consumer started",
		slog.String("token", logToken),
		slog.Time("start_ts", startTs),
	)

	var tokenParam any = token
	if isInitial {
		tokenParam = nil
	}

	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT ChangeRecord FROM READ_%s(
			start_timestamp => @start_ts,
			end_timestamp   => NULL,
			partition_token => @token,
			heartbeat_milliseconds => @heartbeat_ms
		)`, r.streamName),
		Params: map[string]any{
			"start_ts":     startTs,
			"token":        tokenParam,
			"heartbeat_ms": int64(r.heartbeatMS),
		},
	}

	iter := r.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("partition %q: read change stream row: %w", logToken, err)
		}

		var records []*ChangeRecord
		if err := row.Column(0, &records); err != nil {
			return fmt.Errorf("partition %q: decode change record: %w", logToken, err)
		}

		for _, rec := range records {
			for _, dcr := range rec.DataChangeRecords {
				if err := r.handleDataChange(ctx, logToken, dcr); err != nil {
					return err
				}
			}

			for _, hr := range rec.HeartbeatRecords {
				if err := r.handleHeartbeat(ctx, token, hr); err != nil {
					return err
				}
			}

			for _, cpr := range rec.ChildPartitionsRecords {
				if err := r.handleChildPartitions(ctx, token, spawn, cpr); err != nil {
					return err
				}
				// Partition is done after handing off to children.
				return nil
			}
		}
	}

	return nil
}

func (r *Relay) handleDataChange(ctx context.Context, logToken string, dcr *DataChangeRecord) error {
	if dcr.TableName != outboxTable || dcr.ModType != "INSERT" {
		return nil
	}

	type pendingEntry struct {
		entryID string
		videoID string
		topic   string
		payload spanner.NullJSON
	}

	grouped := make(map[string][]pendingEntry)
	for _, mod := range dcr.Mods {
		keysRaw, err := json.Marshal(mod.Keys.Value)
		if err != nil {
			r.logger.Warn(ctx, "outbox_relay.keys_marshal_failed", "Failed to marshal mod keys",
				slog.String("error", err.Error()))
			continue
		}
		var keys map[string]any
		if err := json.Unmarshal(keysRaw, &keys); err != nil {
			r.logger.Warn(ctx, "outbox_relay.keys_unmarshal_failed", "Failed to unmarshal mod keys",
				slog.String("error", err.Error()))
			continue
		}

		valsRaw, err := json.Marshal(mod.NewValues.Value)
		if err != nil {
			r.logger.Warn(ctx, "outbox_relay.vals_marshal_failed", "Failed to marshal mod new_values",
				slog.String("error", err.Error()))
			continue
		}
		var vals map[string]any
		if err := json.Unmarshal(valsRaw, &vals); err != nil {
			r.logger.Warn(ctx, "outbox_relay.vals_unmarshal_failed", "Failed to unmarshal mod new_values",
				slog.String("error", err.Error()))
			continue
		}

		entryID, _ := keys["entry_id"].(string)
		videoID, _ := vals["video_id"].(string)
		topic, _ := vals["topic"].(string)

		if entryID == "" || topic == "" {
			r.logger.Warn(ctx, "outbox_relay.missing_fields",
				"Skipping outbox mod with missing entry_id or topic",
				slog.String("entry_id", entryID),
				slog.String("token", logToken),
			)
			continue
		}

		grouped[topic] = append(grouped[topic], pendingEntry{
			entryID: entryID,
			videoID: videoID,
			topic:   topic,
			payload: spanner.NullJSON{Value: vals["payload"], Valid: vals["payload"] != nil},
		})
	}

	for topic, entries := range grouped {
		var publishedIDs []string

		for _, e := range entries {
			env, data, err := parsePayload(e.payload)
			if err != nil {
				r.logger.Warn(ctx, "outbox_relay.payload_parse_failed",
					"Failed to parse outbox payload; skipping entry",
					slog.String("entry_id", e.entryID),
					slog.String("topic", topic),
					slog.String("error", err.Error()),
				)
				continue
			}

			result := r.publisher.PublishFromOutbox(ctx, topic, env, data, map[string]string{
				"video_id": e.videoID,
			})

			if _, err := result.Get(ctx); err != nil {
				r.logger.Error(ctx, "outbox_relay.publish_failed",
					"Failed to publish outbox entry to Pub/Sub",
					slog.String("entry_id", e.entryID),
					slog.String("topic", topic),
					slog.String("error", err.Error()),
				)
				continue
			}

			publishedIDs = append(publishedIDs, e.entryID)
		}

		if len(publishedIDs) == 0 {
			continue
		}

		if err := store.MarkOutboxEntriesPublished(ctx, r.spannerClient, publishedIDs); err != nil {
			return fmt.Errorf("mark outbox entries published (topic %q): %w", topic, err)
		}

		r.logger.Info(ctx, "outbox_relay.publish_success",
			"Published outbox entries",
			slog.String("topic", topic),
			slog.Int("count", len(publishedIDs)),
		)
	}

	// Advance watermark after processing this transaction's records.
	if token := logToken; token != "<initial>" {
		if err := checkpoint.UpsertCheckpoint(ctx, r.spannerClient, checkpoint.PartitionCheckpoint{
			PartitionToken: token,
			Watermark:      dcr.CommitTimestamp,
			State:          checkpoint.StateActive,
		}); err != nil {
			r.logger.Warn(ctx, "outbox_relay.checkpoint_failed",
				"Failed to update watermark checkpoint",
				slog.String("token", token),
				slog.String("error", err.Error()),
			)
		}
	}

	return nil
}

func (r *Relay) handleHeartbeat(ctx context.Context, token string, hr *HeartbeatRecord) error {
	if token == "" {
		return nil
	}
	if err := checkpoint.UpsertCheckpoint(ctx, r.spannerClient, checkpoint.PartitionCheckpoint{
		PartitionToken: token,
		Watermark:      hr.Timestamp,
		State:          checkpoint.StateActive,
	}); err != nil {
		r.logger.Warn(ctx, "outbox_relay.heartbeat_checkpoint_failed",
			"Failed to update heartbeat checkpoint",
			slog.String("token", token),
			slog.String("error", err.Error()),
		)
	}
	return nil
}

func (r *Relay) handleChildPartitions(ctx context.Context, token string, spawn func(string, time.Time), cpr *ChildPartitionsRecord) error {
	r.logger.Info(ctx, "outbox_relay.partition_split",
		"Partition split detected; spawning child partitions",
		slog.String("parent_token", token),
		slog.Int("child_count", len(cpr.ChildPartitions)),
	)

	for _, child := range cpr.ChildPartitions {
		if err := checkpoint.UpsertCheckpoint(ctx, r.spannerClient, checkpoint.PartitionCheckpoint{
			PartitionToken:        child.Token,
			Watermark:             cpr.StartTimestamp,
			State:                 checkpoint.StateActive,
			ParentPartitionTokens: child.ParentPartitionTokens,
		}); err != nil {
			return fmt.Errorf("upsert child partition checkpoint %q: %w", child.Token, err)
		}
		spawn(child.Token, cpr.StartTimestamp)
	}

	if token != "" {
		if err := checkpoint.MarkPartitionFinished(ctx, r.spannerClient, token); err != nil {
			return fmt.Errorf("mark parent partition finished %q: %w", token, err)
		}
	}

	return nil
}

// parsePayload decodes a Spanner NullJSON outbox payload into an Envelope and
// the raw JSON bytes of the envelope's Data field.
func parsePayload(payload spanner.NullJSON) (outbox.Envelope, []byte, error) {
	if !payload.Valid {
		return outbox.Envelope{}, nil, fmt.Errorf("payload is null")
	}

	// Change streams serialize JSON-typed columns as a JSON string, so the
	// stream path yields a Go string holding the envelope JSON. Direct reads
	// (the sweep path) yield the already-decoded value, which we re-marshal.
	var raw []byte
	switch v := payload.Value.(type) {
	case string:
		raw = []byte(v)
	case []byte:
		raw = v
	default:
		b, err := json.Marshal(payload.Value)
		if err != nil {
			return outbox.Envelope{}, nil, fmt.Errorf("marshal JSON payload: %w", err)
		}
		raw = b
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
