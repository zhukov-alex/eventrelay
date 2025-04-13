package cleaner

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.uber.org/zap"
)

type Config struct {
	WALDir            string
	MaxSegmentsToKeep int
}

type Service struct {
	cfg    *Config
	logger *zap.Logger
}

func NewService(logger *zap.Logger, cfg *Config) *Service {
	return &Service{
		cfg:    cfg,
		logger: logger,
	}
}

func (s *Service) CleanupOldSegments() {
	logger := s.logger.With(zap.String("method", "CleanupOldSegments"))

	entries, err := os.ReadDir(s.cfg.WALDir)
	if err != nil {
		logger.Error("failed to read WAL directory", zap.String("dir", s.cfg.WALDir), zap.Error(err))
		return
	}

	var segments []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".log") {
			segments = append(segments, filepath.Join(s.cfg.WALDir, name))
		}
	}

	sort.Strings(segments)
	total := len(segments)

	if total <= s.cfg.MaxSegmentsToKeep {
		logger.Info("no segments to delete",
			zap.Int("found", total),
			zap.Int("max_segments_to_keep", s.cfg.MaxSegmentsToKeep),
		)
		return
	}

	toDelete := segments[:total-s.cfg.MaxSegmentsToKeep]
	deleted := 0

	for _, path := range toDelete {
		if err := os.Remove(path); err != nil {
			logger.Error("failed to remove WAL segment", zap.String("file", path), zap.Error(err))
		} else {
			deleted++
		}
	}

	logger.Info("WAL cleanup completed",
		zap.Int("total_segments", total),
		zap.Int("deleted_segments", deleted),
		zap.Int("kept_segments", s.cfg.MaxSegmentsToKeep),
	)
}
