package ingest

import (
	"fmt"
	"time"
)

type Config struct {
	Type string     `mapstructure:"type"`
	TCP  *TCPConfig `mapstructure:"tcp"`
	// GRPC *GRPCConfig `mapstructure:"grpc"`
}

type TCPConfig struct {
	BindAddr       string        `mapstructure:"bind_addr"`
	MaxConnections int           `mapstructure:"max_connections"`
	ReadTimeout    time.Duration `mapstructure:"read_timeout"`
}

// type GRPCConfig struct {
// 	BindAddr string `mapstructure:"bind_addr"`
// }

func (c *Config) Validate() error {
	if c.Type == "" {
		return fmt.Errorf("ingest type is required")
	}
	if c.Type == "tcp" {
		if c.TCP == nil {
			return fmt.Errorf("tcp config must be provided for type=tcp")
		}
		if err := c.TCP.Validate(); err != nil {
			return fmt.Errorf("tcp config: %w", err)
		}
	}
	return nil
}

func (t *TCPConfig) Validate() error {
	if t.BindAddr == "" {
		return fmt.Errorf("tcp.bind_addr is required")
	}
	if t.MaxConnections <= 0 {
		return fmt.Errorf("tcp.max_connections must be > 0")
	}
	if t.ReadTimeout <= 0 {
		return fmt.Errorf("tcp.read_timeout must be > 0")
	}
	return nil
}
