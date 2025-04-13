//go:build integration

package ingest_test

import (
	"context"
	"encoding/binary"
	"go.uber.org/zap/zaptest"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zhukov-alex/eventrelay/internal/ingest"
)

type mockRelay struct{}

func (m *mockRelay) Save(data []byte) error          { return nil }
func (m *mockRelay) Start(ctx context.Context) error { return nil }
func (m *mockRelay) Close(ctx context.Context) error { return nil }

func TestTCPServer_ServeAndClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr := "127.0.0.1:19100"

	srv := ingest.NewTCPServer(zaptest.NewLogger(t), &ingest.TCPConfig{
		BindAddr:       addr,
		MaxConnections: 100,
		ReadTimeout:    1 * time.Second,
	}, false)

	go func() {
		err := srv.Serve(ctx, &mockRelay{})
		if err != nil {
			t.Logf("server exited: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer conn.Close()

	msg := []byte("ping")
	length := make([]byte, 4)
	binary.LittleEndian.PutUint32(length, uint32(len(msg)))

	_, err = conn.Write(length)
	assert.NoError(t, err)
	_, err = conn.Write(msg)
	assert.NoError(t, err)

	ack := make([]byte, 1)
	_, err = io.ReadFull(conn, ack)
	assert.NoError(t, err)
	assert.Equal(t, byte(0x00), ack[0])

	err = srv.Close(ctx)
	assert.NoError(t, err)
}
