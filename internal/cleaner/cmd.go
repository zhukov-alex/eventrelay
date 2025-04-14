package cleaner

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zhukov-alex/eventrelay/internal/config"
	"github.com/zhukov-alex/eventrelay/internal/logger"
)

const EnvStage = "ENVIRONMENT"

func CleanupCmd(_ *cobra.Command, _ []string) error {
	var cfgFile string
	cobra.OnInitialize(config.NewConfigInit(&cfgFile))

	cfg, err := config.New(viper.GetViper())
	if err != nil {
		return fmt.Errorf("create config: %w", err)
	}
	var devMode = strings.ToLower(os.Getenv(EnvStage)) != "prod"
	l, err := logger.New(cfg.Logger, devMode)
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer l.Sync()

	if cfg.MaxSegmentsToKeep <= 0 {
		l.Info("Skipping cleanup: max_segments_to_keep is zero or unset.")
		return nil
	}

	cleanCfg := &Config{
		WALDir:            cfg.Relay.WALDir,
		MaxSegmentsToKeep: cfg.MaxSegmentsToKeep,
	}

	svc := NewService(l, cleanCfg)
	svc.CleanupOldSegments()

	return nil
}
