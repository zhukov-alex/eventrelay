package relay_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	"github.com/zhukov-alex/eventrelay/internal/mocks"
	"github.com/zhukov-alex/eventrelay/internal/output"
	"github.com/zhukov-alex/eventrelay/internal/relay"
)

func setupService(t *testing.T, ctrl *gomock.Controller) (*relay.Config, *mocks.MockWalWriter, *mocks.MockOutput, relay.Service, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta")
	assert.NoError(t, os.WriteFile(metaPath, []byte("0:0"), 0644))

	mockWal := mocks.NewMockWalWriter(ctrl)
	mockOut := mocks.NewMockOutput(ctrl)

	cfg := &relay.Config{
		InChannelSize:    10,
		OutputBatchSize:  1,
		WALSegmentSizeMB: 1,
		SyncOnWrite:      true,
		FlushInterval:    10 * time.Millisecond,
		WorkerID:         "worker-test",
		WALDir:           tmpDir,
		MetaPath:         metaPath,
	}

	logger := zaptest.NewLogger(t)

	svc := relay.New(logger, *cfg, mockWal, mockOut, false)
	return cfg, mockWal, mockOut, svc, func() { _ = svc.Close(context.Background()) }
}

func expectWalCommon(w *mocks.MockWalWriter) {
	w.EXPECT().Flush().Return(nil).AnyTimes()
	w.EXPECT().Close().Return(nil).AnyTimes()
	w.EXPECT().Open(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
}

func TestService_Start_Stop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, mockWal, mockOut, svc, cleanup := setupService(t, ctrl)
	defer cleanup()

	expectWalCommon(mockWal)
	mockOut.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	assert.NoError(t, svc.Start(context.Background()))
}

func TestService_Save_Success(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	message := []byte("test-message")
	expectedPayload := message

	batchSent := make(chan struct{})

	_, mockWal, mockOut, svc, cleanup := setupService(t, ctrl)
	defer cleanup()

	expectWalCommon(mockWal)

	mockWal.EXPECT().Write(gomock.AssignableToTypeOf([]byte{})).DoAndReturn(func(data []byte) (int, error) {
		if len(data) < 4+len(expectedPayload) {
			t.Errorf("Write received too little data: %d bytes", len(data))
		}
		actualPayload := data[4:]
		assert.Equal(t, expectedPayload, actualPayload, "payload mismatch")
		return len(data), nil
	})

	mockOut.EXPECT().SendBatch(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, msg output.OutputBatchMessage) error {
		assert.Len(t, msg.Batch, 1)
		assert.Equal(t, expectedPayload, msg.Batch[0].Payload)
		close(batchSent)
		return nil
	}).Times(1)

	assert.NoError(t, svc.Start(context.Background()))
	assert.NoError(t, svc.Save(message))

	select {
	case <-batchSent:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for SendBatch")
	}
}

func TestService_Save_ContextCancelled(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, mockWal, _, svc, cleanup := setupService(t, ctrl)
	defer cleanup()

	expectWalCommon(mockWal)

	assert.NoError(t, svc.Start(context.Background()))
	_ = svc.Close(context.Background())
	err := svc.Save([]byte("cancelled"))
	assert.Equal(t, context.Canceled, err)
}

func TestService_Save_TriggersSegmentRotation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg, mockWal, mockOut, svc, cleanup := setupService(t, ctrl)
	defer cleanup()

	cfg.WALSegmentSizeMB = 1
	largeMessage := make([]byte, 2*1024*1024)

	mockWal.EXPECT().Flush().Return(nil).AnyTimes()
	mockWal.EXPECT().Close().Return(nil).AnyTimes()
	mockWal.EXPECT().Open(gomock.Any(), gomock.Any()).Return(nil).Times(2)
	mockWal.EXPECT().Write(gomock.AssignableToTypeOf([]byte{})).Return(len(largeMessage)+4, nil)
	mockOut.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(nil)

	assert.NoError(t, svc.Start(context.Background()))
	assert.NoError(t, svc.Save(largeMessage))
	time.Sleep(100 * time.Millisecond)
}

func TestService_SendBatchRetry(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	message := []byte("retry-this")
	retryDone := make(chan struct{})

	_, mockWal, mockOut, svc, cleanup := setupService(t, ctrl)
	defer cleanup()

	expectWalCommon(mockWal)

	mockWal.EXPECT().Write(gomock.Any()).DoAndReturn(func(data []byte) (int, error) {
		actualPayload := data[4:] // skip length prefix
		assert.Equal(t, message, actualPayload)
		return len(data), nil
	})

	gomock.InOrder(
		mockOut.EXPECT().SendBatch(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, msg output.OutputBatchMessage) error {
			assert.Len(t, msg.Batch, 1)
			assert.Equal(t, message, msg.Batch[0].Payload)
			return assert.AnError // simulate retry trigger
		}),
		mockOut.EXPECT().SendBatch(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, msg output.OutputBatchMessage) error {
			assert.Len(t, msg.Batch, 1)
			assert.Equal(t, message, msg.Batch[0].Payload)
			close(retryDone)
			return nil
		}),
	)

	assert.NoError(t, svc.Start(context.Background()))
	assert.NoError(t, svc.Save(message))

	select {
	case <-retryDone:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout: retry SendBatch was not called")
	}
}

func TestService_Save_AfterRunStopped_DoesNotHang(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, mockWal, _, svc, _ := setupService(t, ctrl)
	expectWalCommon(mockWal)

	assert.NoError(t, svc.Start(context.Background()))
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.NoError(t, svc.Close(ctx))

	done := make(chan struct{})
	go func() {
		_ = svc.Save([]byte("after-stop"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Save() is stuck - expected it to exit after context cancellation")
	}
}

func TestService_Save_WriteError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, mockWal, _, svc, cleanup := setupService(t, ctrl)
	defer cleanup()

	mockWal.EXPECT().Open(gomock.Any(), gomock.Any()).Return(nil)
	mockWal.EXPECT().Write(gomock.Any()).Return(0, fmt.Errorf("write failed"))
	mockWal.EXPECT().Flush().Return(nil).AnyTimes()
	mockWal.EXPECT().Close().Return(nil).AnyTimes()

	assert.NoError(t, svc.Start(context.Background()))
	err := svc.Save([]byte("fail"))
	assert.ErrorContains(t, err, "write failed")
}

func TestService_Save_FlushError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, mockWal, mockOut, svc, cleanup := setupService(t, ctrl)
	defer cleanup()

	var flushCalledDuringSave bool
	var flushShouldFail bool

	mockWal.EXPECT().Open(gomock.Any(), gomock.Any()).Return(nil)
	mockWal.EXPECT().Write(gomock.Any()).Return(10, nil).AnyTimes()
	mockWal.EXPECT().Close().Return(nil).AnyTimes()

	mockWal.EXPECT().Flush().DoAndReturn(func() error {
		if flushShouldFail {
			flushCalledDuringSave = true
			return fmt.Errorf("flush failed")
		}
		return nil
	}).AnyTimes()

	mockOut.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Times(0)

	assert.NoError(t, svc.Start(context.Background()))

	flushShouldFail = true
	err := svc.Save([]byte("flush-error"))

	assert.True(t, flushCalledDuringSave, "Flush was not called during Save()")
	assert.ErrorContains(t, err, "flush failed")
}

func TestService_ManyMessages_Batched(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		totalMessages   = 100
		batchSize       = 10
		expectedBatches = totalMessages / batchSize
	)

	batchesSent := make(chan output.OutputBatchMessage, expectedBatches)

	cfg, mockWal, mockOut, svc, cleanup := setupService(t, ctrl)
	defer cleanup()

	cfg.OutputBatchSize = batchSize
	expectWalCommon(mockWal)

	mockWal.EXPECT().Write(gomock.Any()).DoAndReturn(func(data []byte) (int, error) {
		if len(data) < 4 {
			t.Errorf("Write: data too short: %d bytes", len(data))
		}
		return len(data), nil
	}).AnyTimes()

	mockOut.EXPECT().SendBatch(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, msg output.OutputBatchMessage) error {
			assert.NotEmpty(t, msg.Batch)
			for _, m := range msg.Batch {
				assert.Contains(t, string(m.Payload), "msg-")
			}
			select {
			case batchesSent <- msg:
			default:
			}
			return nil
		},
	).MinTimes(expectedBatches)

	assert.NoError(t, svc.Start(context.Background()))

	for i := 0; i < totalMessages; i++ {
		assert.NoError(t, svc.Save([]byte(fmt.Sprintf("msg-%d", i))))
	}

	timeout := time.After(1 * time.Second)
	received := 0
	for received < expectedBatches {
		select {
		case <-batchesSent:
			received++
		case <-timeout:
			t.Fatalf("timeout: received only %d batches, expected %d", received, expectedBatches)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.NoError(t, svc.Close(ctx))
}
