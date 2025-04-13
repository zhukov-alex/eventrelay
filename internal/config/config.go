package config

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/viper"

	"github.com/zhukov-alex/eventrelay/internal/ingest"
	"github.com/zhukov-alex/eventrelay/internal/logger"
	"github.com/zhukov-alex/eventrelay/internal/output"
	"github.com/zhukov-alex/eventrelay/internal/relay"
)

type Config struct {
	WALType           string `mapstructure:"wal_type"`
	MetricsAddr       string `mapstructure:"metrics_addr"`
	MaxSegmentsToKeep int    `mapstructure:"max_segments_to_keep"`

	Relay  relay.Config  `mapstructure:"relay"`
	Ingest ingest.Config `mapstructure:"ingest"`
	Output output.Config `mapstructure:"output"`
	Logger logger.Config `mapstructure:"logger"`
}

func NewConfigInit(cfgFile *string) func() {
	return func() {
		if strings.TrimSpace(*cfgFile) == "" {
			log.Fatalf("invalid config file name")
		}
		if _, err := os.Stat(*cfgFile); err != nil {
			log.Fatalf("invalid config path: %v", err)
		}
		viper.SetConfigFile(*cfgFile)

		if err := viper.ReadInConfig(); err != nil {
			log.Fatalf("Failed to read config: %v\n", err)
		}
	}
}

func New(v *viper.Viper) (*Config, error) {
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("config unmarshal error: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation error: %w", err)
	}
	return &cfg, nil
}

func (c *Config) Validate() error {
	if err := c.Logger.Validate(); err != nil {
		return fmt.Errorf("logger config: %w", err)
	}
	if c.WALType == "" {
		return fmt.Errorf("wal_type is required")
	}
	// 0 - disables deletion of old WAL segments
	if c.MaxSegmentsToKeep < 0 {
		return fmt.Errorf("max_segments_to_keep must be >= 0")
	}
	if err := c.Relay.Validate(); err != nil {
		return fmt.Errorf("relay config: %w", err)
	}
	if err := c.Ingest.Validate(); err != nil {
		return fmt.Errorf("ingest config: %w", err)
	}
	if err := c.Output.Validate(); err != nil {
		return fmt.Errorf("output config: %w", err)
	}
	return nil
}
