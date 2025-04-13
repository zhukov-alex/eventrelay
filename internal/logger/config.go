package logger

import (
	"fmt"
)

type Config struct {
	EnableWriteToFile bool   `mapstructure:"enable_write_to_file"`
	FilePath          string `mapstructure:"file_path"`
	MaxSize           int    `mapstructure:"max_size"` // in MB
	MaxBackups        int    `mapstructure:"max_backups"`
	MaxAgeDays        int    `mapstructure:"max_age_days"`
}

func (c *Config) Validate() error {
	if c.EnableWriteToFile {
		if c.FilePath == "" {
			return fmt.Errorf("file_path is required")
		}
		if c.MaxSize <= 0 {
			return fmt.Errorf("max_size must be > 0")
		}
		if c.MaxBackups < 0 {
			return fmt.Errorf("max_backups must be >= 0")
		}
		if c.MaxAgeDays < 0 {
			return fmt.Errorf("max_age_days must be >= 0")
		}
	}
	return nil
}
