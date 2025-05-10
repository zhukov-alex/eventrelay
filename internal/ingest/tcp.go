package ingest

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/zhukov-alex/eventrelay/internal/relay"
	"go.uber.org/zap"
)

var lengthBufPool = sync.Pool{
	New: func() any { return make([]byte, 4) },
}

var eventBufPool = sync.Pool{
	New: func() any { return make([]byte, 64*1024) },
}

type TCPServer struct {
	cfg       *TCPConfig
	metrics   *metrics
	listener  net.Listener
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	logger    *zap.Logger
}

func NewTCPServer(logger *zap.Logger, cfg *TCPConfig, registerMetrics bool) *TCPServer {
	return &TCPServer{
		cfg:     cfg,
		metrics: initMetrics(registerMetrics),
		logger:  logger,
	}
}

func (s *TCPServer) Serve(ctx context.Context, repl relay.Service) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	listener, err := net.Listen("tcp", s.cfg.BindAddr)
	if err != nil {
		return err
	}
	s.listener = listener

	sem := make(chan struct{}, s.cfg.MaxConnections)
	s.logger.Info("TCP server started", zap.String("addr", s.cfg.BindAddr))

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || s.ctx.Err() != nil {
				return nil // graceful shutdown
			}

			s.logger.Error("tcp accept failed", zap.Error(err))
			s.metrics.incTcpError()
			continue
		}

		select {
		case sem <- struct{}{}:
			s.wg.Add(1)
			go func(c net.Conn) {
				defer func() {
					<-sem
					s.wg.Done()
				}()
				s.handleTCPConn(c, repl)
			}(conn)
		default:
			s.logger.Warn("too many connections - rejecting client")
			conn.Close()
		}
	}
}

func (s *TCPServer) Close(ctx context.Context) error {
	var err error
	s.closeOnce.Do(func() {
		s.logger.Info("TCPServer shutting down...")

		if s.cancel != nil {
			s.cancel()
		}
		if s.listener != nil {
			err = s.listener.Close()
		}

		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			s.logger.Info("TCPServer shutdown complete")
		case <-ctx.Done():
			err = ctx.Err()
			s.logger.Warn("TCPServer shutdown timeout", zap.Error(err))
		}
	})
	return err
}

func (s *TCPServer) handleTCPConn(conn net.Conn, repl relay.Service) {
	defer conn.Close()

	logger := s.logger.With(zap.String("method", "handleTCPConn"))

	for {
		select {
		case <-s.ctx.Done():
			logger.Info("context canceled - closing connection")
			return
		default:
		}

		startIngestTime := time.Now()

		_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))

		lengthBuf := lengthBufPool.Get().([]byte)
		_, err := io.ReadFull(conn, lengthBuf)
		lengthBufPool.Put(lengthBuf)

		if err != nil {
			if os.IsTimeout(err) {
				logger.Warn("timeout reading length")
			} else if err != io.EOF {
				logger.Error("read length error", zap.Error(err))
			}
			s.metrics.incTcpError()
			return
		}

		length := binary.LittleEndian.Uint32(lengthBuf)
		if length == 0 || length > MaxEventSize {
			logger.Warn("invalid event size", zap.Uint32("size", length))
			s.metrics.incTcpError()
			return
		}

		raw := eventBufPool.Get().([]byte)
		if cap(raw) < int(length) {
			raw = make([]byte, length)
		}
		eventBuf := raw[:length]

		_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		_, err = io.ReadFull(conn, eventBuf)
		if err != nil {
			logger.Error("read body error", zap.Error(err))
			s.metrics.incTcpError()
			eventBufPool.Put(raw)
			return
		}

		msg := make([]byte, length)
		copy(msg, eventBuf)
		eventBufPool.Put(raw)

		if err := repl.Save(msg); err != nil {
			logger.Error("save error", zap.Error(err))
			s.metrics.incTcpError()
			return
		}

		_ = conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		_, err = conn.Write([]byte{0x00})
		if err != nil {
			logger.Error("failed to write ACK", zap.Error(err))
			s.metrics.incTcpError()
		}

		s.metrics.tcpIngestLatency.Observe(time.Since(startIngestTime).Seconds())
	}
}
