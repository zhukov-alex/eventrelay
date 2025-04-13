package output

import "context"

type OutputMessage struct {
	ID      string
	Payload []byte
}

type OutputBatchMessage struct {
	Batch    []OutputMessage
	WorkerID string
}

type Output interface {
	Send(ctx context.Context, msg OutputMessage, workerID string) error
	SendBatch(ctx context.Context, batch OutputBatchMessage) error
	Close(ctx context.Context) error
}
