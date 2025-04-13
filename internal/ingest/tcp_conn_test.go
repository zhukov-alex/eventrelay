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

func TestTCPServer_ConnIsolation(t *testing.T) {
	addr := "127.0.0.1:19200"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := ingest.NewTCPServer(zaptest.NewLogger(t), &ingest.TCPConfig{
		BindAddr:       addr,
		MaxConnections: 300,
		ReadTimeout:    1 * time.Second,
	}, false)
	go func() {
		err := srv.Serve(ctx, &mockRelay{})
		if err != nil {
			t.Logf("server exited: %v", err)
		}
	}()

	time.Sleep(300 * time.Millisecond) // allow server to start

	numClients := 1000
	errCh := make(chan error, numClients)
	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errCh <- fmt.Errorf("conn %d dial failed: %w", i, err)
				return
			}
			defer conn.Close()

			msg := []byte(fmt.Sprintf("hello-%d", i))
			frame := make([]byte, 4+len(msg))
			binary.LittleEndian.PutUint32(frame[:4], uint32(len(msg)))
			copy(frame[4:], msg)

			if _, err := conn.Write(frame); err != nil {
				errCh <- fmt.Errorf("conn %d write failed: %w", i, err)
				return
			}

			ack := make([]byte, 1)
			_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			if _, err := conn.Read(ack); err != nil || ack[0] != 0x00 {
				errCh <- fmt.Errorf("conn %d ack failed: %v", i, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("client error: %v", err)
	}
}
