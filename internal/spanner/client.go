package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
)

type Config struct {
	NumChannels           int
	EnableEndToEndTracing bool
	DatabaseRole          string
	DisableRouteToLeader  bool
}

func DefaultConfig() Config {
	return Config{
		NumChannels:           4,
		EnableEndToEndTracing: true,
	}
}

func NewClient(ctx context.Context, database string, cfg Config, opts ...option.ClientOption) (*spanner.Client, error) {

	if database == "" {
		return nil, fmt.Errorf("database is required")
	}

	spannerCfg := spanner.ClientConfig{
		EnableEndToEndTracing: cfg.EnableEndToEndTracing,
		DatabaseRole:          cfg.DatabaseRole,
		DisableRouteToLeader:  cfg.DisableRouteToLeader,
	}

	if cfg.NumChannels > 0 {
		opts = append(opts, option.WithGRPCConnectionPool(cfg.NumChannels))
	}

	client, err := spanner.NewClientWithConfig(ctx, database, spannerCfg, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create spanner client(%s): %w", database, err)
	}

	return client, nil
}
