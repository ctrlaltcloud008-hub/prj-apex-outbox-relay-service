package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/api/option"
)

type Config struct {
	ProjectID      string
	EnabledTracing bool
	Endpoint       string
}

func NewClient(ctx context.Context, cfg Config) (*pubsub.Client, error) {
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("project ID is required")
	}

	clientCfg := pubsub.ClientConfig{
		EnableOpenTelemetryTracing: cfg.EnabledTracing,
	}

	var opts []option.ClientOption
	if cfg.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(cfg.Endpoint))
	}

	client, err := pubsub.NewClientWithConfig(ctx, cfg.ProjectID, &clientCfg, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client(%s): %w", cfg.ProjectID, err)
	}

	return client, nil
}
