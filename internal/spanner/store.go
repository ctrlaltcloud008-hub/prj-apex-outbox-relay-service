package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	spannerutil "github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/spanner"
	"google.golang.org/api/iterator"
)

type PendingOutboxEntry struct {
	EntryID   string
	VideoID   string
	Topic     string
	Payload   spanner.NullJSON
	CreatedAt time.Time
}

func ReadPendingOutboxEntries(ctx context.Context, client *spanner.Client, shardID int64, batchSize int64) ([]PendingOutboxEntry, error) {

	stmt := spanner.Statement{
		SQL: `
			SELECT entry_id, video_id, topic, payload, created_at
			FROM outbox
			WHERE shard_id = @shard AND status = 'PENDING'
			ORDER BY created_at ASC
			LIMIT @batch_size
		`,
		Params: map[string]any{
			"shard":      shardID,
			"batch_size": batchSize,
		},
	}

	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	entries := make([]PendingOutboxEntry, 0, batchSize)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("query pending outbox entries: %w", err)
		}
		var entry PendingOutboxEntry
		if err := row.Columns(
			&entry.EntryID,
			&entry.VideoID,
			&entry.Topic,
			&entry.Payload,
			&entry.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("read pending outbox row: %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil

}

func MarkOutboxEntriesPublished(ctx context.Context, client *spanner.Client, entryIDs []string) error {
	if len(entryIDs) == 0 {
		return nil
	}

	publishedAt := time.Now().UTC()

	_, err := spannerutil.RunRW(ctx, client, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutations := make([]*spanner.Mutation, 0, len(entryIDs))
		for _, entryID := range entryIDs {
			mutations = append(mutations, spanner.Update("outbox",
				[]string{"entry_id", "status", "published_at"},
				[]any{entryID, "PUBLISHED", publishedAt},
			))
		}

		if err := txn.BufferWrite(mutations); err != nil {
			return fmt.Errorf("buffer outbox publish updates: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("mark outbox entries published: %w", err)
	}

	return nil
}
