package handler

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/logger"
	store "github.com/ctrlaltcloud008-hub/prj-apex-outbox-poller-service/internal/spanner"
)

// CleanupHandler deletes PUBLISHED outbox rows past the retention window.
// Triggered daily by Cloud Scheduler.
type CleanupHandler struct {
	logger        *logger.Logger
	spannerClient *spanner.Client
	retention     time.Duration
}

func NewCleanupHandler(log *logger.Logger, spannerClient *spanner.Client, retention time.Duration) *CleanupHandler {
	return &CleanupHandler{
		logger:        log,
		spannerClient: spannerClient,
		retention:     retention,
	}
}

func (h *CleanupHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	deleted, err := store.CleanupPublished(ctx, h.spannerClient, h.retention)
	if err != nil {
		h.logger.Error(ctx, "outbox_cleanup.failed", "Partitioned DML cleanup failed",
			slog.String("error", err.Error()),
		)
		http.Error(w, "cleanup failed", http.StatusInternalServerError)
		return
	}

	h.logger.Info(ctx, "outbox_cleanup.completed", "Deleted PUBLISHED outbox rows past retention",
		slog.Int64("rows_deleted", deleted),
		slog.Duration("retention", h.retention),
	)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"rows_deleted": %d}`, deleted)
}
