package config

import (
	"strings"

	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/config"
	"github.com/spf13/viper"
)

type OutboxPollerConfig struct {
	appEnv         string
	port           string
	service        string
	region         string
	projectID      string
	spannerDB      string
	batchSize      int64
	pollIntervalMS int
}

func LoadOutboxPollerConfig() (*OutboxPollerConfig, error) {

	v := viper.New()
	v.SetDefault("PORT", "8080")
	v.SetDefault("APP_ENV", "local")
	v.SetDefault("SERVICE", "ingestion")
	v.SetDefault("REGION", "asia-south1")
	v.SetDefault("PROJECT_ID", "test-project")
	v.SetDefault("SPANNER_DATABASE", "projects/test-project/instances/test-instance/databases/test-database")
	v.SetDefault("BATCH_SIZE", 100)
	v.SetDefault("POLL_INTERVAL_MS", 5000)

	if err := config.LoadConfig(v, "config"); err != nil {
		return nil, err
	}

	cfg := &OutboxPollerConfig{
		appEnv:         v.GetString("APP_ENV"),
		port:           normalizePort(v.GetString("PORT")),
		service:        v.GetString("SERVICE"),
		region:         v.GetString("REGION"),
		projectID:      v.GetString("PROJECT_ID"),
		spannerDB:      v.GetString("SPANNER_DATABASE"),
		batchSize:      v.GetInt64("BATCH_SIZE"),
		pollIntervalMS: v.GetInt("POLL_INTERVAL_MS"),
	}

	return cfg, nil
}

func normalizePort(port string) string {
	port = strings.TrimSpace(port)
	if port == "" {
		return ":8080"
	}
	if strings.HasPrefix(port, ":") {
		return port
	}
	return ":" + port
}

func (c *OutboxPollerConfig) AppEnv() string          { return c.appEnv }
func (c *OutboxPollerConfig) Port() string            { return c.port }
func (c *OutboxPollerConfig) Service() string         { return c.service }
func (c *OutboxPollerConfig) Region() string          { return c.region }
func (c *OutboxPollerConfig) ProjectID() string       { return c.projectID }
func (c *OutboxPollerConfig) SpannerDatabase() string { return c.spannerDB }
func (c *OutboxPollerConfig) BatchSize() int64        { return c.batchSize }
func (c *OutboxPollerConfig) PollIntervalMS() int     { return c.pollIntervalMS }
