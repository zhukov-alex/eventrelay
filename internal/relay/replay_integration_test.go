package relay

import (
	"context"
	"encoding/binary"
	"go.uber.org/zap/zaptest"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReplay_SingleSegment(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta")
	assert.NoError(t, os.WriteFile(metaPath, []byte("0:0"), 0644))

	segmentPath := filepath.Join(tmpDir, "00000001.log")
	file, err := os.Create(segmentPath)
	assert.NoError(t, err)
	defer file.Close()

	// write two messages in WAL format: [len][data]
	msgs := [][]byte{
		[]byte("hello"),
		[]byte("world"),
	}
	for _, msg := range msgs {
		length := uint32(len(msg))
		assert.NoError(t, binary.Write(file, binary.LittleEndian, length))
		_, err := file.Write(msg)
		assert.NoError(t, err)
	}
	assert.NoError(t, file.Sync())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &ServiceImpl{
		ctx:           ctx,
		cfg:           Config{WALDir: tmpDir, MetaPath: metaPath, OutputBatchSize: 2},
		cursorSegment: 1,
		cursorOffset:  0,
		outCh:         make(chan batchToOut, 1),
		logger:        zaptest.NewLogger(t),
	}

	err = s.replayWAL()
	assert.NoError(t, err)

	select {
	case batch := <-s.outCh:
		assert.Equal(t, 2, len(batch.batch))
		assert.Equal(t, msgs[0], batch.batch[0].Payload)
		assert.Equal(t, msgs[1], batch.batch[1].Payload)
	case <-time.After(time.Second):
		t.Fatal("timeout: expected replayed batch")
	}
}

func TestReplay_BatchFlush(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "00000042.log")
	file, err := os.Create(segmentPath)
	assert.NoError(t, err)
	defer file.Close()

	// write 21 messages
	var allMsgs [][]byte
	for i := 0; i < 21; i++ {
		msg := []byte("msg-" + strconv.Itoa(i))
		length := uint32(len(msg))
		assert.NoError(t, binary.Write(file, binary.LittleEndian, length))
		_, err := file.Write(msg)
		assert.NoError(t, err)
		allMsgs = append(allMsgs, msg)
	}
	assert.NoError(t, file.Sync())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan batchToOut, 3)
	s := &ServiceImpl{
		ctx:    ctx,
		cfg:    Config{OutputBatchSize: 10},
		outCh:  out,
		logger: zaptest.NewLogger(t),
	}

	err = s.replaySegment(segmentPath, 42, 0, true)
	assert.NoError(t, err)

	total := 0
	for i := 0; i < 3; i++ {
		select {
		case batch := <-out:
			total += len(batch.batch)
			for _, m := range batch.batch {
				assert.Contains(t, string(m.Payload), "msg-")
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout: expected batch #%d", i+1)
		}
	}
	assert.Equal(t, 21, total)
}

func TestReplay_ContextCanceled(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "00000099.log")
	file, err := os.Create(segmentPath)
	assert.NoError(t, err)
	defer file.Close()

	// Write 5 valid messages
	for i := 0; i < 5; i++ {
		msg := []byte("early-msg-" + strconv.Itoa(i))
		length := uint32(len(msg))
		assert.NoError(t, binary.Write(file, binary.LittleEndian, length))
		_, err := file.Write(msg)
		assert.NoError(t, err)
	}
	assert.NoError(t, file.Sync())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	out := make(chan batchToOut, 1)
	s := &ServiceImpl{
		ctx:    ctx,
		cfg:    Config{OutputBatchSize: 2},
		outCh:  out,
		logger: zaptest.NewLogger(t),
	}

	err = s.replaySegment(segmentPath, 99, 0, true)
	assert.NoError(t, err)

	select {
	case <-out:
		t.Fatal("unexpected batch received after context cancel")
	case <-time.After(100 * time.Millisecond):
		// success: nothing received
	}
}

func TestReplay_InvalidLengthPrefix(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "00000077.log")
	file, err := os.Create(segmentPath)
	assert.NoError(t, err)
	defer file.Close()

	// write length = 10, but only 3 bytes of payload (simulate corrupt/truncated entry)
	length := uint32(10)
	assert.NoError(t, binary.Write(file, binary.LittleEndian, length))
	_, err = file.Write([]byte("abc"))
	assert.NoError(t, err)
	assert.NoError(t, file.Sync())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &ServiceImpl{
		ctx:    ctx,
		cfg:    Config{OutputBatchSize: 5},
		outCh:  make(chan batchToOut, 1),
		logger: zaptest.NewLogger(t),
	}

	err = s.replaySegment(segmentPath, 77, 0, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reading log entry error")
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
