package relay

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zhukov-alex/eventrelay/internal/output"
	"go.uber.org/zap"
)

func (s *ServiceImpl) replayWAL() error {
	logger := s.logger.With(zap.String("method", "replayWAL"))

	// Discover all WAL segment files in the directory
	mask := filepath.Join(s.cfg.WALDir, "*.log")
	segPaths, err := filepath.Glob(mask)
	if err != nil {
		logger.Error("failed to scan WAL directory", zap.String("pattern", mask), zap.Error(err))
		return fmt.Errorf("replayWAL: glob error: %w", err)
	}

	// Sort the file paths to ensure sequential processing
	sort.Strings(segPaths)

	logger.Info("replaying WAL segments", zap.Int("segments_total", len(segPaths)))

	// Helper to extract segment number from file path
	parseSegment := func(path string) (uint64, error) {
		base := filepath.Base(path)
		numPart := strings.TrimSuffix(base, ".log")
		return strconv.ParseUint(numPart, 10, 64)
	}

	for i, segPath := range segPaths {
		segment, err := parseSegment(segPath)
		if err != nil {
			logger.Error("failed to parse segment filename", zap.String("file", segPath), zap.Error(err))
			return fmt.Errorf("replayWAL: can't parse segment %s: %w", segPath, err)
		}

		// Skip segments that are older than the current replay position
		if segment < s.cursorSegment {
			continue
		}

		// Determine the offset to start reading from
		var offset uint64
		if segment == s.cursorSegment {
			offset = s.cursorOffset
		} else {
			offset = 0
		}

		// Check if this is the last segment in the list
		isLast := i == len(segPaths)-1

		logger.Info("replaying segment", zap.String("file", segPath), zap.Uint64("segment", segment), zap.Uint64("offset", offset))

		// Replay this segment from the given offset
		if err := s.replaySegment(segPath, segment, offset, isLast); err != nil {
			logger.Error("replay segment failed", zap.String("file", segPath), zap.Error(err))
			return err
		}
	}

	logger.Info("WAL replay complete")
	return nil
}

// replaySegment replays a single WAL segment from the specified offset
func (s *ServiceImpl) replaySegment(path string, segment uint64, offset uint64, isLast bool) error {
	logger := s.logger.With(zap.String("method", "replaySegment"))

	batchSize := s.cfg.OutputBatchSize

	select {
	case <-s.ctx.Done():
		return nil
	default:
	}

	f, err := os.Open(path)
	if err != nil {
		logger.Error("failed to open WAL segment", zap.String("path", path), zap.Error(err))
		return fmt.Errorf("replayWAL: open file %s error: %w", path, err)
	}
	defer f.Close()

	// Seek to the correct offset in the file
	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		logger.Error("failed to seek in WAL segment", zap.String("path", path), zap.Uint64("offset", offset), zap.Error(err))
		return fmt.Errorf("replayWAL: seek file %s error: %w", path, err)
	}

	reader := bufio.NewReader(f)
	var batch []output.OutputMessage
	idgen := newIDGen()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}

		// Read 4-byte length prefix
		lenBuf := make([]byte, 4)
		_, err := io.ReadFull(reader, lenBuf)
		if err != nil {
			if err == io.EOF {
				break // reached end of file
			}
			logger.Error("failed to read message length", zap.String("path", path), zap.Uint64("offset", offset), zap.Error(err))
			return fmt.Errorf("replayWAL: reading length error in file %s: %w", path, err)
		}

		// Read the actual payload of the given length
		length := binary.LittleEndian.Uint32(lenBuf)
		logBuf := make([]byte, length)
		_, err = io.ReadFull(reader, logBuf)
		if err != nil {
			logger.Error("failed to read WAL message", zap.String("path", path), zap.Uint64("offset", offset), zap.Error(err))
			return fmt.Errorf("replayWAL: reading log entry error in file %s: %w", path, err)
		}

		batch = append(batch, output.OutputMessage{
			ID:      fmt.Sprintf("%x", idgen.Next()),
			Payload: logBuf,
		})
		offset += 4 + uint64(length)

		// Flush batch if size exceeded
		if len(batch) >= batchSize {
			s.outCh <- batchToOut{
				batch:   batch,
				segment: segment,
				offset:  offset,
			}
			batch = nil
		}
	}

	// If not the last segment, advance to the next segment with offset 0
	if !isLast {
		segment++
		offset = 0
	}

	// Send remaining batch if any
	if len(batch) > 0 {
		s.outCh <- batchToOut{
			batch:   batch,
			segment: segment,
			offset:  offset,
		}
	}
	return nil
}

func (s *ServiceImpl) waitForReplay() {
	if len(s.outCh) == 0 {
		return
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if len(s.outCh) == 0 {
				return
			}
		}
	}
}
