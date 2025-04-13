package logger

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	EnvLogLevel = "LOG_LEVEL"
	EnvTimeFmt  = "LOG_TIME"
)

func New(cfg Config, devMode bool) (*zap.Logger, error) {
	level := zapcore.DebugLevel
	if raw := os.Getenv(EnvLogLevel); raw != "" {
		if err := level.UnmarshalText([]byte(strings.ToLower(raw))); err != nil {
			return nil, fmt.Errorf("invalid LOG_LEVEL (expected one of: debug, info, warn, error etc): %w", err)
		}
	}

	var timeEncoder zapcore.TimeEncoder = zapcore.EpochTimeEncoder
	if raw := os.Getenv(EnvTimeFmt); raw != "" {
		if err := timeEncoder.UnmarshalText([]byte(raw)); err != nil {
			return nil, fmt.Errorf("invalid LOG_TIME (expected one of: epoch, iso8601, rfc3339 etc): %w", err)
		}
	}

	zapCfg := zap.NewProductionEncoderConfig()
	if devMode {
		zapCfg = zap.NewDevelopmentEncoderConfig()
	}
	zapCfg.EncodeTime = timeEncoder
	encoder := zapcore.NewJSONEncoder(zapCfg)

	writers := []zapcore.WriteSyncer{zapcore.AddSync(os.Stdout)}
	if cfg.EnableWriteToFile {
		writers = append(writers, zapcore.AddSync(&lumberjack.Logger{
			Filename:   cfg.FilePath,
			MaxSize:    cfg.MaxSize,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAgeDays,
		}))
	}

	core := zapcore.NewCore(encoder, zapcore.NewMultiWriteSyncer(writers...), level)
	return zap.New(core, zap.WithCaller(true)), nil
}
