package app

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/zhukov-alex/eventrelay/internal/config"
	"github.com/zhukov-alex/eventrelay/internal/ingest"
	"github.com/zhukov-alex/eventrelay/internal/logger"
	"github.com/zhukov-alex/eventrelay/internal/metrics"
	"github.com/zhukov-alex/eventrelay/internal/output"
	"github.com/zhukov-alex/eventrelay/internal/relay"
	"github.com/zhukov-alex/eventrelay/internal/wal"
)

const EnvStage = "ENVIRONMENT"

func RelayCmd(_ *cobra.Command, _ []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	collectMetrics := cfg.MetricsAddr != ""
	var metricsCloser func(ctx context.Context) error
	if collectMetrics {
		metricsSrv, cl := metrics.New(l, cfg.MetricsAddr)
		metricsSrv.Start()
		metricsCloser = cl
	}

	outp, err := func() (output.Output, error) {
		switch cfg.Output.Type {
		case "kafka":
			if cfg.Output.Kafka == nil {
				return nil, fmt.Errorf("kafka config is missing")
			}
			return output.NewKafkaBroker(l, cfg.Output.Kafka)
		default:
			return nil, fmt.Errorf("unsupported output type: %s", cfg.Output.Type)
		}
	}()
	if err != nil {
		return fmt.Errorf("output init error: %w", err)
	}

	var ww wal.WalWriter
	switch cfg.WALType {
	case "buffered":
		ww = wal.NewBufferedWriter()
	//case "mmap":
	//	ww = wal.NewMmapWriter()
	default:
		return fmt.Errorf("unsupported wal writer: %q", cfg.WALType)
	}

	relaySrv := relay.New(l, cfg.Relay, ww, outp, collectMetrics)
	if err := relaySrv.Start(ctx); err != nil {
		return fmt.Errorf("failed to start relay: %w", err)
	}

	ingestor, err := func() (ingest.Ingest, error) {
		switch cfg.Ingest.Type {
		case "tcp":
			if cfg.Ingest.TCP == nil {
				return nil, fmt.Errorf("tcp config is missing")
			}
			return ingest.NewTCPServer(l, cfg.Ingest.TCP, collectMetrics), nil
		//case "grpc":
		//	if cfg.Ingest.GRPC == nil {
		//		return nil, fmt.Errorf("grpc config is missing")
		//	}
		//	return ingest.NewGRPCServer(cfg.Ingest.GRPC, collectMetrics), nil
		default:
			return nil, fmt.Errorf("unsupported ingest type: %s", cfg.Ingest.Type)
		}
	}()
	if err != nil {
		return fmt.Errorf("ingest init error: %w", err)
	}

	go func() {
		if err := ingestor.Serve(ctx, relaySrv); err != nil {
			l.Error("server error", zap.Error(err))
		}
	}()

	<-ctx.Done()
	l.Info("Shutdown signal received")

	clCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := ingestor.Close(clCtx); err != nil {
		l.Error("error shutting down ingestor", zap.Error(err))
	}

	g, ctx := errgroup.WithContext(clCtx)
	g.Go(func() error { return relaySrv.Close(ctx) })
	g.Go(func() error { return outp.Close(ctx) })
	if collectMetrics {
		g.Go(func() error { return metricsCloser(ctx) })
	}

	if err := g.Wait(); err != nil {
		l.Error("shutdown errors", zap.Error(err))
	} else {
		l.Info("Shutdown complete")
	}

	return nil
}
