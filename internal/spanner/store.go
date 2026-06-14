package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	spannerutil "github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/spanner"
	"google.golang.org/api/iterator"
)

// PendingEntry is an outbox row that has not been published yet.
type PendingEntry struct {
	EntryID string
	VideoID string
	Topic   string
	Payload spanner.NullJSON
}

// MarkOutboxEntriesPublished flips entries to PUBLISHED. The update is
// conditional on status='PENDING' so replayed change-stream records or a
// concurrent sweep marking the same entry can never error out a partition
// consumer (a blind mutation on a missing row returns NotFound and kills it).
func MarkOutboxEntriesPublished(ctx context.Context, client *spanner.Client, entryIDs []string) error {
	if len(entryIDs) == 0 {
		return nil
	}

	_, err := spannerutil.RunRW(ctx, client, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE outbox
				SET status = 'PUBLISHED', published_at = CURRENT_TIMESTAMP()
				WHERE entry_id IN UNNEST(@ids) AND status = 'PENDING'`,
			Params: map[string]any{"ids": entryIDs},
		})
		if err != nil {
			return fmt.Errorf("update outbox publish status: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("mark outbox entries published: %w", err)
	}

	return nil
}

// ScanPendingEntries returns PENDING outbox rows older than minAge for one
// shard, oldest first. The (shard_id, status, created_at) index serves this
// query; callers iterate shards 0..shardCount-1.
func ScanPendingEntries(ctx context.Context, client *spanner.Client, shardID int64, minAge time.Duration, limit int64) ([]PendingEntry, error) {
	stmt := spanner.Statement{
		SQL: `SELECT entry_id, video_id, topic, payload
			FROM outbox
			WHERE shard_id = @shard
			  AND status = 'PENDING'
			  AND created_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minAgeSec SECOND)
			ORDER BY created_at
			LIMIT @lim`,
		Params: map[string]any{
			"shard":     shardID,
			"minAgeSec": int64(minAge.Seconds()),
			"lim":       limit,
		},
	}

	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var entries []PendingEntry
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("scan pending outbox entries: %w", err)
		}

		var e PendingEntry
		if err := row.Columns(&e.EntryID, &e.VideoID, &e.Topic, &e.Payload); err != nil {
			return nil, fmt.Errorf("read pending outbox row: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// CleanupPublished hard-deletes PUBLISHED rows older than the retention window
// using Partitioned DML, which parallelizes across splits and sidesteps the
// per-transaction mutation limit. Returns the number of rows deleted.
func CleanupPublished(ctx context.Context, client *spanner.Client, retention time.Duration) (int64, error) {
	count, err := client.PartitionedUpdate(ctx, spanner.Statement{
		SQL: `DELETE FROM outbox
			WHERE status = 'PUBLISHED'
			  AND published_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @retentionHours HOUR)`,
		Params: map[string]any{
			"retentionHours": int64(retention.Hours()),
		},
	})
	if err != nil {
		return 0, fmt.Errorf("cleanup published outbox rows: %w", err)
	}
	return count, nil
}
