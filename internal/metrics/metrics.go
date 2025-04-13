package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type Server struct {
	addr   string
	srv    *http.Server
	logger *zap.Logger
}

func New(logger *zap.Logger, addr string) (*Server, func(ctx context.Context) error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         addr,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		Handler:      mux,
	}

	server := &Server{
		addr:   addr,
		srv:    srv,
		logger: logger,
	}

	closer := func(ctx context.Context) error {
		logger.Info("Shutting down metrics server...")
		return srv.Shutdown(ctx)
	}

	return server, closer
}

func (m *Server) Start() {
	go func() {
		m.logger.Info("Metrics server started", zap.String("addr", m.addr))
		if err := m.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.logger.Error("Metrics server error", zap.Error(err))
		}
	}()
}
