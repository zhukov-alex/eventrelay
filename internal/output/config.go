package output

import (
	"fmt"
	"time"
)

type Config struct {
	Type  string       `mapstructure:"type"`
	Kafka *KafkaConfig `mapstructure:"kafka"`
}

type KafkaConfig struct {
	Brokers           []string      `mapstructure:"brokers"`
	Topic             string        `mapstructure:"topic"`
	Acks              string        `mapstructure:"acks"`
	FlushMessages     int           `mapstructure:"flush_messages"`
	FlushFrequency    time.Duration `mapstructure:"flush_frequency"`
	ChannelBufferSize int           `mapstructure:"channel_buffer_size"`
}

func (c *Config) Validate() error {
	if c.Type == "" {
		return fmt.Errorf("output type is required")
	}
	if c.Type == "kafka" {
		if c.Kafka == nil {
			return fmt.Errorf("kafka config must be provided for type=kafka")
		}
		if err := c.Kafka.Validate(); err != nil {
			return fmt.Errorf("kafka config: %w", err)
		}
	}
	return nil
}

func (k *KafkaConfig) Validate() error {
	if len(k.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers must not be empty")
	}
	if k.Topic == "" {
		return fmt.Errorf("kafka.topic is required")
	}
	if k.Acks != "0" && k.Acks != "1" && k.Acks != "all" {
		return fmt.Errorf("kafka.acks must be one of: 0, 1, all")
	}
	if k.FlushMessages <= 0 {
		return fmt.Errorf("kafka.flush_messages must be > 0")
	}
	if k.FlushFrequency <= 0 {
		return fmt.Errorf("kafka.flush_frequency must be > 0")
	}
	if k.ChannelBufferSize <= 0 {
		return fmt.Errorf("kafka.channel_buffer_size must be > 0")
	}
	return nil
}
