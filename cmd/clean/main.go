package main

import (
	"log"

	"github.com/spf13/cobra"
	"github.com/zhukov-alex/eventrelay/internal/cleaner"
	"github.com/zhukov-alex/eventrelay/internal/config"
)

func main() {
	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime | log.Lmicroseconds)

	var cfgFile string
	cobra.OnInitialize(config.NewConfigInit(&cfgFile))

	cmd := &cobra.Command{
		Use:   "clean-wal",
		Short: "Clean up old WAL segments",
		RunE:  cleaner.CleanupCmd,
	}

	cmd.Flags().StringVar(&cfgFile, "config", "config/config.yaml", "Path to the configuration file (default: config/config.yaml)")

	if err := cmd.Execute(); err != nil {
		log.Fatalf("command error: %v", err)
	}
}
