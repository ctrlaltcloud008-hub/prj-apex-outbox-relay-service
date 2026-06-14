package config

import (
	"strings"

	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/config"
	"github.com/spf13/viper"
)

type OutboxRelayConfig struct {
	appEnv                string
	port                  string
	service               string
	region                string
	projectID             string
	spannerDB             string
	changeStreamName      string
	heartbeatIntervalMS   int
	startLookbackSecs     int
	sweepIntervalSecs     int
	sweepMinAgeSecs       int
	publishedRetentionHrs int
}

func LoadOutboxRelayConfig() (*OutboxRelayConfig, error) {

	v := viper.New()
	v.SetDefault("PORT", "8080")
	v.SetDefault("APP_ENV", "local")
	v.SetDefault("SERVICE", "outbox-relay")
	v.SetDefault("REGION", "asia-south1")
	v.SetDefault("PROJECT_ID", "test-project")
	v.SetDefault("SPANNER_DATABASE", "projects/test-project/instances/test-instance/databases/test-database")
	v.SetDefault("CHANGE_STREAM_NAME", "outbox_stream")
	v.SetDefault("HEARTBEAT_INTERVAL_MS", 10000)
	v.SetDefault("START_LOOKBACK_SECONDS", 60)
	// PENDING sweep: safety net for entries the change-stream path missed.
	v.SetDefault("SWEEP_INTERVAL_SECONDS", 30)
	v.SetDefault("SWEEP_MIN_AGE_SECONDS", 30)
	// Retention for PUBLISHED rows, deleted by the /cleanup endpoint (7 days).
	v.SetDefault("PUBLISHED_RETENTION_HOURS", 168)

	if err := config.LoadConfig(v, "config"); err != nil {
		return nil, err
	}

	cfg := &OutboxRelayConfig{
		appEnv:                v.GetString("APP_ENV"),
		port:                  normalizePort(v.GetString("PORT")),
		service:               v.GetString("SERVICE"),
		region:                v.GetString("REGION"),
		projectID:             v.GetString("PROJECT_ID"),
		spannerDB:             v.GetString("SPANNER_DATABASE"),
		changeStreamName:      v.GetString("CHANGE_STREAM_NAME"),
		heartbeatIntervalMS:   v.GetInt("HEARTBEAT_INTERVAL_MS"),
		startLookbackSecs:     v.GetInt("START_LOOKBACK_SECONDS"),
		sweepIntervalSecs:     v.GetInt("SWEEP_INTERVAL_SECONDS"),
		sweepMinAgeSecs:       v.GetInt("SWEEP_MIN_AGE_SECONDS"),
		publishedRetentionHrs: v.GetInt("PUBLISHED_RETENTION_HOURS"),
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

func (c *OutboxRelayConfig) AppEnv() string             { return c.appEnv }
func (c *OutboxRelayConfig) Port() string               { return c.port }
func (c *OutboxRelayConfig) Service() string            { return c.service }
func (c *OutboxRelayConfig) Region() string             { return c.region }
func (c *OutboxRelayConfig) ProjectID() string          { return c.projectID }
func (c *OutboxRelayConfig) SpannerDatabase() string    { return c.spannerDB }
func (c *OutboxRelayConfig) ChangeStreamName() string    { return c.changeStreamName }
func (c *OutboxRelayConfig) HeartbeatIntervalMS() int    { return c.heartbeatIntervalMS }
func (c *OutboxRelayConfig) StartLookbackSecs() int      { return c.startLookbackSecs }
func (c *OutboxRelayConfig) SweepIntervalSecs() int      { return c.sweepIntervalSecs }
func (c *OutboxRelayConfig) SweepMinAgeSecs() int        { return c.sweepMinAgeSecs }
func (c *OutboxRelayConfig) PublishedRetentionHrs() int  { return c.publishedRetentionHrs }
