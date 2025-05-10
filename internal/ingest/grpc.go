package ingest

import (
    "context"
    "io"
    "net"
    "time"

    "go.uber.org/zap"
    "google.golang.org/grpc"

    "github.com/zhukov-alex/eventrelay/internal/relay"
    pb "github.com/zhukov-alex/eventrelay/proto/ingestpb"
)

type GRPCServer struct {
    cfg          *GRPCConfig
    metrics      *metrics
    relayService relay.Service
    logger       *zap.Logger
    server       *grpc.Server
}

func NewGRPCServer(logger *zap.Logger, cfg *GRPCConfig, registerMetrics bool) *GRPCServer {
    server := grpc.NewServer(
        grpc.ConnectionTimeout(cfg.ReadTimeout),
        grpc.MaxRecvMsgSize(MaxEventSize+1024),
    )
    metrics := initMetrics(registerMetrics)
    return &GRPCServer{
        cfg:     cfg,
        metrics: metrics,
        logger:  logger,
        server:  server,
    }
}

func (s *GRPCServer) Serve(ctx context.Context, repl relay.Service) error {
    s.relayService = repl
    pb.RegisterIngestorServer(s.server, &ingestorService{
        repl:    s.relayService,
        logger:  s.logger,
        timeout: s.cfg.ReadTimeout,
        metrics: s.metrics,
    })

    lis, err := net.Listen("tcp", s.cfg.BindAddr)
    if err != nil {
        return err
    }
    s.logger.Info("gRPC server started", zap.String("addr", s.cfg.BindAddr))

    go func() {
        <-ctx.Done()
        s.server.GracefulStop()
    }()

    err = s.server.Serve(lis)
    if err == grpc.ErrServerStopped {
        return nil
    }
    return err
}

func (s *GRPCServer) Close(ctx context.Context) error {
    if s.server != nil {
        s.server.GracefulStop()
    }
    return nil
}

type ingestorService struct {
    pb.UnimplementedIngestorServer
    repl    relay.Service
    logger  *zap.Logger
    timeout time.Duration
    metrics *metrics
}

// StreamLogs handles incoming log events from the client stream.
func (i *ingestorService) StreamLogs(stream pb.Ingestor_StreamLogsServer) error {
    for {
        msg, err := stream.Recv()
        if err == io.EOF {
            return nil
        }
        if err != nil {
            i.logger.Error("stream recv failed", zap.Error(err))
            i.metrics.grpcIngestErrors.Inc()
            return err
        }

        // validate payload size
        if len(msg.Data) == 0 || len(msg.Data) > MaxEventSize {
            i.logger.Warn("invalid event size", zap.Int("size", len(msg.Data)))
            i.metrics.grpcIngestErrors.Inc()
            if sendErr := stream.Send(&pb.Ack{Ok: false}); sendErr != nil {
                i.logger.Error("failed to send invalid-size ack", zap.Error(sendErr))
                return sendErr
            }
            continue
        }

        // process event
        startIngestTime := time.Now()
        if err := i.repl.Save(msg.Data); err != nil {
            i.logger.Error("relay save failed", zap.Error(err))
            i.metrics.grpcIngestErrors.Inc()
            if sendErr := stream.Send(&pb.Ack{Ok: false}); sendErr != nil {
                i.metrics.grpcIngestErrors.Inc()
                return sendErr
            }
            continue
        }

        // acknowledge
        if err := stream.Send(&pb.Ack{Ok: true}); err != nil {
            i.logger.Error("stream send failed", zap.Error(err))
            i.metrics.grpcIngestErrors.Inc()
            return err
        }

        i.metrics.grpcIngestLatency.Observe(time.Since(startIngestTime).Seconds())
    }
}
