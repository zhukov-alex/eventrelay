package ingest

import (
	"fmt"
	"time"
)

type Config struct {
	Type string     `mapstructure:"type"`
	TCP  *TCPConfig `mapstructure:"tcp"`
	GRPC *GRPCConfig `mapstructure:"grpc"`
}

type TCPConfig struct {
	BindAddr       string        `mapstructure:"bind_addr"`
	MaxConnections int           `mapstructure:"max_connections"`
	ReadTimeout    time.Duration `mapstructure:"read_timeout"`
}

type GRPCConfig struct {
	BindAddr    string        `mapstructure:"bind_addr"`
	ReadTimeout time.Duration `mapstructure:"read_timeout"`
}

func (c *Config) Validate() error {
	if c.Type == "" {
		return fmt.Errorf("ingest type is required")
	}
	switch c.Type {
	case "tcp":
		if c.TCP == nil {
			return fmt.Errorf("tcp config must be provided for type=tcp")
		}
		if err := c.TCP.Validate(); err != nil {
			return fmt.Errorf("tcp config: %w", err)
		}
	case "grpc":
		if c.GRPC == nil {
			return fmt.Errorf("grpc config must be provided for type=grpc")
		}
		if err := c.GRPC.Validate(); err != nil {
			return fmt.Errorf("grpc config: %w", err)
		}
	default:
		return fmt.Errorf("unsupported ingest type: %s", c.Type)
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

func (g *GRPCConfig) Validate() error {
	if g.BindAddr == "" {
		return fmt.Errorf("grpc.bind_addr is required")
	}
	if g.ReadTimeout <= 0 {
		return fmt.Errorf("grpc.read_timeout must be > 0")
	}
	return nil
}

const MaxEventSize = 10 * 1024 * 1024 // 10mb
