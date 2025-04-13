package ingest

import (
	"context"
	"github.com/zhukov-alex/eventrelay/internal/relay"
)

type Ingest interface {
	Serve(ctx context.Context, repl relay.Service) error
	Close(ctx context.Context) error
}
