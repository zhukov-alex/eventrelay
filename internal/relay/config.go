package relay

import (
	"fmt"
	"time"
)

type Config struct {
	WorkerID         string        `mapstructure:"worker_id"`
	MetaPath         string        `mapstructure:"meta_path"`
	WALDir           string        `mapstructure:"wal_dir"`
	WALSegmentSizeMB uint64        `mapstructure:"wal_segment_size_mb"`
	InChannelSize    int           `mapstructure:"in_channel_size"`
	SyncOnWrite      bool          `mapstructure:"sync_on_write"`
	FlushInterval    time.Duration `mapstructure:"flush_interval"`
	OutputBatchSize  int           `mapstructure:"output_batch_size"`
}

func (c *Config) Validate() error {
	if c.MetaPath == "" {
		return fmt.Errorf("meta_path is required")
	}
	if c.WALDir == "" {
		return fmt.Errorf("wal_dir is required")
	}
	if c.WALSegmentSizeMB == 0 {
		return fmt.Errorf("wal_segment_size_mb must be > 0")
	}
	if c.InChannelSize <= 0 {
		return fmt.Errorf("in_channel_size must be > 0")
	}
	if c.FlushInterval <= 0 {
		return fmt.Errorf("flush_interval must be > 0")
	}
	if c.OutputBatchSize <= 0 {
		return fmt.Errorf("output_batch_size must be > 0")
	}
	return nil
}
