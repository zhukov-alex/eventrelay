package relay

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zhukov-alex/eventrelay/internal/output"
	writer "github.com/zhukov-alex/eventrelay/internal/wal"
	"go.uber.org/zap"
)

// Service defines the interface for the relay service.
type Service interface {
	Start(ctx context.Context) error
	Save(data []byte) error
	Close(ctx context.Context) error
}

type inRequest struct {
	data []byte
	err  chan error
}

type batchToOut struct {
	batch   []output.OutputMessage
	segment uint64
	offset  uint64
}

type ServiceImpl struct {
	cfg           Config
	wal           writer.WalWriter
	output        output.Output
	inCh          chan inRequest
	outCh         chan batchToOut
	cursorSegment uint64
	cursorOffset  uint64
	metrics       *metrics
	ctx           context.Context
	cancel        context.CancelFunc
	closeOnce     sync.Once
	wg            sync.WaitGroup
	logger        *zap.Logger
}

var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// New creates a new instance of the relay service.
func New(logger *zap.Logger, cfg Config, ww writer.WalWriter, out output.Output, registerMetrics bool) Service {
	return &ServiceImpl{
		cfg:     cfg,
		wal:     ww,
		output:  out,
		inCh:    make(chan inRequest, cfg.InChannelSize),
		outCh:   make(chan batchToOut, cfg.OutChannelSize),
		metrics: initMetrics(registerMetrics),
		logger:  logger,
	}
}

// Start initializes and starts the relay service.
func (s *ServiceImpl) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	s.logger.Info("starting relay service")

	if err := os.MkdirAll(s.cfg.WALDir, 0755); err != nil {
		s.cancel()
		return fmt.Errorf("failed to create WAL directory: %w", err)
	}

	var err error
	s.cursorSegment, s.cursorOffset, err = loadMeta(s.cfg.MetaPath)
	if err != nil {
		s.cancel()
		return fmt.Errorf("loadMeta error: %w", err)
	}

	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.dispatchLoop() }()

	if err := s.replayWAL(); err != nil {
		s.cancel()
		return fmt.Errorf("replayWAL failed: %w", err)
	}
	s.waitForReplay()

	segmentFile := s.segmentFilename(s.cursorSegment)
	if err := s.wal.Open(segmentFile, int64(s.cursorOffset)); err != nil {
		s.cancel()
		return fmt.Errorf("failed to open WAL file: %w", err)
	}

	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.run() }()

	return nil
}

func (s *ServiceImpl) Save(data []byte) error {
	req := inRequest{
		data: data,
		err:  make(chan error, 1),
	}
	select {
	case <-s.ctx.Done():
		return context.Canceled
	case s.inCh <- req:
		select {
		case err := <-req.err:
			return err
		case <-s.ctx.Done():
			return context.Canceled
		}
	}
}

func (s *ServiceImpl) Close(ctx context.Context) error {
	var err error
	s.closeOnce.Do(func() {
		s.logger.Info("relay service shutting down...")

		if s.cancel != nil {
			s.cancel()
		}

		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			s.logger.Info("relay service shutdown complete.")
		case <-ctx.Done():
			err = ctx.Err()
			s.logger.Warn("relay service shutdown timeout", zap.Error(err))
		}
	})
	return err
}

func (s *ServiceImpl) segmentFilename(segment uint64) string {
	return filepath.Join(s.cfg.WALDir, fmt.Sprintf("%08d.log", segment))
}

func saveMeta(metaPath string, segment uint64, offset uint64) error {
	return os.WriteFile(metaPath, []byte(fmt.Sprintf("%d:%d", segment, offset)), 0644)
}

func loadMeta(metaPath string) (segment uint64, offset uint64, err error) {
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("loadMeta: failed to read meta file: %w", err)
	}

	parts := strings.Split(string(data), ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("loadMeta: invalid format in meta file: %q", string(data))
	}

	seg, err1 := strconv.ParseUint(parts[0], 10, 64)
	off, err2 := strconv.ParseUint(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, fmt.Errorf("loadMeta: failed to parse segment or offset: %v / %v", err1, err2)
	}

	return seg, off, nil
}

func (s *ServiceImpl) dispatchLoop() {
	logger := s.logger.With(zap.String("method", "dispatchLoop"))

	var curBatch *batchToOut
	var retryDelay time.Duration
	const maxDelay = 3 * time.Second

	for {
		if curBatch == nil {
			select {
			case <-s.ctx.Done():
				return
			case b := <-s.outCh:
				curBatch = &b
				retryDelay = 0
			}
		} else {
			select {
			case <-s.ctx.Done():
				return
			default:
			}

			stopBatchTimer := s.metrics.kafkaBatchTimer()
			err := s.output.SendBatch(s.ctx, output.OutputBatchMessage{
				Batch:    curBatch.batch,
				WorkerID: s.cfg.WorkerID,
			})
			if err != nil {
				logger.Error("sendOutputBatch failed", zap.Error(err))
				retryDelay = nextBackoff(retryDelay, maxDelay)
				select {
				case <-time.After(retryDelay):
					continue
				case <-s.ctx.Done():
					return
				}
			}
			logger.Info("batch sent to output OK")
			stopBatchTimer()

			if err := saveMeta(s.cfg.MetaPath, curBatch.segment, curBatch.offset); err != nil {
				logger.Error("saveMeta failed, stopping", zap.Error(err))
				s.cancel()
				return
			}
			s.cursorSegment = curBatch.segment
			s.cursorOffset = curBatch.offset

			curBatch = nil
		}
	}
}

func (s *ServiceImpl) run() {
	logger := s.logger.With(zap.String("method", "run"))
	defer close(s.outCh)

	interval := time.Duration(0)
	if !s.cfg.SyncOnWrite {
		interval = s.cfg.FlushInterval
	}
	tickerChan, ticker := makeTickerChan(interval)
	if ticker != nil {
		defer ticker.Stop()
	}

	var batch []output.OutputMessage
	idgen := newIDGen()

	curSegment := s.cursorSegment
	curOffset := s.cursorOffset

	for {
		select {
		case req := <-s.inCh:
			buf := bufPool.Get().(*bytes.Buffer)
			buf.Reset()

			msgSize := 4 + len(req.data)
			if curOffset+uint64(msgSize) > s.cfg.WALSegmentSizeMB*1024*1024 {
				curSegment++
				curOffset = 0
				if err := s.rotateSegment(curSegment); err != nil {
					logger.Error("rotateSegment error", zap.Error(err))
					req.err <- err
					s.cancel()
					return
				}
			}

			if err := binary.Write(buf, binary.LittleEndian, uint32(len(req.data))); err != nil {
				logger.Error("write length error", zap.Error(err))
				bufPool.Put(buf)
				req.err <- err
				continue
			}
			if _, err := buf.Write(req.data); err != nil {
				logger.Error("write reqData error", zap.Error(err))
				bufPool.Put(buf)
				req.err <- err
				continue
			}

			startWalTime := time.Now() // metrics: wal_write_latency_seconds

			if _, err := s.wal.Write(buf.Bytes()); err != nil {
				logger.Error("writer.Write error", zap.Error(err))
				bufPool.Put(buf)
				req.err <- err
				continue
			}
			if s.cfg.SyncOnWrite {
				if err := s.wal.Flush(); err != nil {
					logger.Error("writer.Flush error (SyncOnWrite)", zap.Error(err))
					bufPool.Put(buf)
					req.err <- err
					continue
				}
			}
			// Message is considered acknowledged after WAL write (and flush if required)
			req.err <- nil

			s.metrics.walWriteLatency.Observe(time.Since(startWalTime).Seconds())

			curOffset += uint64(msgSize)
			bufPool.Put(buf)

			batch = append(batch, output.OutputMessage{
				ID:      fmt.Sprintf("%x", idgen.Next()),
				Payload: req.data,
			})

			if len(batch) >= s.cfg.OutputBatchSize {
				s.outCh <- batchToOut{
					batch:   batch,
					segment: curSegment,
					offset:  curOffset,
				}
				batch = nil
			}

		case <-tickerChan:
			// Flush any pending batch on each tick.
			if err := s.wal.Flush(); err != nil {
				logger.Error("writer.Flush error", zap.Error(err))
			}
			if len(batch) > 0 {
				s.outCh <- batchToOut{
					batch:   batch,
					segment: curSegment,
					offset:  curOffset,
				}
				batch = nil
			}

		case <-s.ctx.Done():
			if err := s.wal.Flush(); err != nil {
				logger.Error("final writer.Flush error", zap.Error(err))
			}
			if err := s.wal.Close(); err != nil {
				logger.Error("wal.Close error", zap.Error(err))
			}
			return
		}
	}
}

// rotateSegment closes the current WAL, increments the segment, and opens a new WAL file.
func (s *ServiceImpl) rotateSegment(segment uint64) error {
	if err := s.wal.Flush(); err != nil {
		return fmt.Errorf("rotateSegment: writer.Flush error: %w", err)
	}
	if err := s.wal.Close(); err != nil {
		s.logger.Error("writer.Close error", zap.Error(err))
		return nil
	}

	newFile := s.segmentFilename(segment)
	if err := s.wal.Open(newFile, 0); err != nil {
		return fmt.Errorf("rotateSegment: failed to open new WAL file %q: %w", newFile, err)
	}
	return nil
}
