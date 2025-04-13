//go:build loadtest

package ingest_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"go.uber.org/zap/zaptest"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/zhukov-alex/eventrelay/internal/ingest"
)

type mockRelay struct{}

func (m *mockRelay) Save(data []byte) error          { return nil }
func (m *mockRelay) Start(ctx context.Context) error { return nil }
func (m *mockRelay) Close(ctx context.Context) error { return nil }

func TestTCPServer_HighRPS(t *testing.T) {
	const (
		addr        = "127.0.0.1:19300"
		numConns    = 10
		msgsPerConn = 1000 // 10 conns * 1000 = 10k
		payload     = "load-test-msg"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		srv := ingest.NewTCPServer(zaptest.NewLogger(t), &ingest.TCPConfig{
			BindAddr:       addr,
			MaxConnections: 100,
			ReadTimeout:    5 * time.Second,
		}, false)
		err := srv.Serve(ctx, &mockRelay{})
		if err != nil {
			t.Logf("server exited: %v", err)
		}
	}()

	time.Sleep(300 * time.Millisecond)

	var wg sync.WaitGroup
	errCh := make(chan error, numConns)
	start := time.Now()

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errCh <- fmt.Errorf("conn %d dial failed: %w", id, err)
				return
			}
			defer conn.Close()

			for j := 0; j < msgsPerConn; j++ {
				msg := []byte(fmt.Sprintf("%s-%d-%d", payload, id, j))
				frame := make([]byte, 4+len(msg))
				binary.LittleEndian.PutUint32(frame[:4], uint32(len(msg)))
				copy(frame[4:], msg)

				if _, err := conn.Write(frame); err != nil {
					errCh <- fmt.Errorf("conn %d write err: %w", id, err)
					return
				}

				ack := make([]byte, 1)
				conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				if _, err := conn.Read(ack); err != nil || ack[0] != 0x00 {
					errCh <- fmt.Errorf("conn %d ack err: %v", id, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	totalDuration := time.Since(start).Seconds()
	close(errCh)

	totalMessages := float64(numConns * msgsPerConn)
	rps := totalMessages / totalDuration

	t.Logf("Processed %.0f messages in %.2fs (%.0f RPS)", totalMessages, totalDuration, rps)

	for err := range errCh {
		t.Errorf("error: %v", err)
	}
}
