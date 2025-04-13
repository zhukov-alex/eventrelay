package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	"github.com/zhukov-alex/eventrelay/internal/mocks"
)

func TestHandleTCPConn_TimeoutDisconnect(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSvc := mocks.NewMockService(ctrl)
	cfg := &TCPConfig{ReadTimeout: 300 * time.Millisecond}
	s := &TCPServer{
		cfg:     cfg,
		metrics: initMetrics(false),
		logger:  zaptest.NewLogger(t),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan struct{})
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		s.handleTCPConn(server, mockSvc)
		close(done)
	}()

	select {
	case <-done:
		// OK - connection closed on timeout
	case <-time.After(1 * time.Second):
		t.Fatal("connection was not closed after timeout")
	}
}

func TestHandleTCPConn_SaveErrorHandling(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSvc := mocks.NewMockService(ctrl)
	cfg := &TCPConfig{ReadTimeout: 1 * time.Second}
	s := &TCPServer{
		cfg:     cfg,
		metrics: initMetrics(false),
		logger:  zaptest.NewLogger(t),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(5))
	buf.Write([]byte("error"))

	mockSvc.EXPECT().Save([]byte("error")).Return(assert.AnError)

	done := make(chan struct{})
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		s.handleTCPConn(server, mockSvc)
		close(done)
	}()

	_, err := client.Write(buf.Bytes())
	assert.NoError(t, err)

	select {
	case <-done:
		// OK - exited after Save error
	case <-time.After(1 * time.Second):
		t.Fatal("handler did not exit after Save error")
	}

	client.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	ack := make([]byte, 1)
	_, err = client.Read(ack)
	assert.Error(t, err)
}

func TestHandleTCPConn_InvalidLength(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSvc := mocks.NewMockService(ctrl)

	cfg := &TCPConfig{
		ReadTimeout:    500 * time.Millisecond,
		MaxConnections: 10,
	}
	s := &TCPServer{
		cfg:     cfg,
		metrics: initMetrics(false),
		logger:  zaptest.NewLogger(t),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	t.Run("length=0", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))

		done := make(chan struct{})
		go func() {
			s.handleTCPConn(server, mockSvc)
			close(done)
		}()

		_, err := client.Write(buf.Bytes())
		assert.NoError(t, err)

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("handler did not close on zero length")
		}

		// ensure no ACK
		client.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		_, err = client.Read(make([]byte, 1))
		assert.Error(t, err)
	})

	t.Run("length=too_large", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, uint32(MaxEventSize+1))

		done := make(chan struct{})
		go func() {
			s.handleTCPConn(server, mockSvc)
			close(done)
		}()

		_, err := client.Write(buf.Bytes())
		assert.NoError(t, err)

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("handler did not close on oversized length")
		}
	})
}
