package ingest_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zhukov-alex/eventrelay/internal/ingest"
	"github.com/zhukov-alex/eventrelay/internal/mocks"
	"github.com/zhukov-alex/eventrelay/internal/relay"
	pb "github.com/zhukov-alex/eventrelay/proto/ingestpb"
)

func TestGRPCServer_StreamLogs(t *testing.T) {
	type testCase struct {
		name       string
		data       [][]byte
		setupMock  func(ctrl *gomock.Controller) relay.Service
		expectAcks []bool
	}

	tests := []testCase{
		{
			name: "single OK",
			data: [][]byte{[]byte("valid-event")},
			setupMock: func(ctrl *gomock.Controller) relay.Service {
				m := mocks.NewMockService(ctrl)
				m.EXPECT().Save(gomock.Any()).Return(nil)
				return m
			},
			expectAcks: []bool{true},
		},
		{
			name: "single Save error",
			data: [][]byte{[]byte("bad-event")},
			setupMock: func(ctrl *gomock.Controller) relay.Service {
				m := mocks.NewMockService(ctrl)
				m.EXPECT().Save(gomock.Any()).Return(fmt.Errorf("fail"))
				return m
			},
			expectAcks: []bool{false},
		},
		{
			name: "multiple events",
			data: [][]byte{[]byte("one"), []byte("two")},
			setupMock: func(ctrl *gomock.Controller) relay.Service {
				m := mocks.NewMockService(ctrl)
				gomock.InOrder(
					m.EXPECT().Save([]byte("one")).Return(nil),
					m.EXPECT().Save([]byte("two")).Return(nil),
				)
				return m
			},
			expectAcks: []bool{true, true},
		},
		{
			name: "too large",
			data: [][]byte{make([]byte, ingest.MaxEventSize+1)},
			setupMock: func(ctrl *gomock.Controller) relay.Service {
				m := mocks.NewMockService(ctrl)
				return m
			},
			expectAcks: []bool{false},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			relayService := tc.setupMock(ctrl)

			port := freePort()
			addr := fmt.Sprintf("127.0.0.1:%d", port)

			srv := ingest.NewGRPCServer(zaptest.NewLogger(t), &ingest.GRPCConfig{
				BindAddr:    addr,
				ReadTimeout: 2 * time.Second,
			}, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// start server
			go func() {
				if err := srv.Serve(ctx, relayService); err != nil {
					t.Logf("server stopped: %v", err)
				}
			}()

			// wait for readiness
			dialCtx, dialCancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer dialCancel()
			var conn *grpc.ClientConn
			var err error
			for {
				conn, err = grpc.DialContext(dialCtx, addr,
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithBlock(),
				)
				if err == nil || dialCtx.Err() != nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			require.NoError(t, err)
			defer conn.Close()

			client := pb.NewIngestorClient(conn)
			stream, err := client.StreamLogs(context.Background())
			require.NoError(t, err)

			// send and receive acks
			for i, data := range tc.data {
				err = stream.Send(&pb.LogEvent{Data: data})
				require.NoError(t, err)

				ack, err := stream.Recv()
				require.NoError(t, err)
				assert.Equal(t, tc.expectAcks[i], ack.GetOk())
			}

			err = stream.CloseSend()
			require.NoError(t, err)

			_, err = stream.Recv()
			assert.Equal(t, io.EOF, err)
		})
	}
}

func freePort() int {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
