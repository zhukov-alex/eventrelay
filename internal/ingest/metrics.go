package ingest

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	tcpIngestLatency  prometheus.Histogram
	tcpIngestErrors   prometheus.Counter
	grpcIngestLatency prometheus.Histogram
	grpcIngestErrors  prometheus.Counter
}

func initMetrics(register bool) *metrics {
	m := &metrics{
		tcpIngestLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "eventrelay",
			Subsystem: "ingest",
			Name:      "tcp_latency_seconds",
			Help:      "Time to read, parse and save a TCP event",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
		}),
		tcpIngestErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "eventrelay",
			Subsystem: "ingest",
			Name:      "tcp_errors_total",
			Help:      "Total number of errors during TCP ingest",
		}),
		grpcIngestLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "eventrelay",
			Subsystem: "ingest",
			Name:      "grpc_latency_seconds",
			Help:      "Time to receive and process a gRPC ingest event",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
		}),
		grpcIngestErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "eventrelay",
			Subsystem: "ingest",
			Name:      "grpc_errors_total",
			Help:      "Total number of errors during gRPC ingest",
		}),
	}

	if register {
		prometheus.MustRegister(
			m.tcpIngestLatency,
			m.tcpIngestErrors,
			m.grpcIngestLatency,
			m.grpcIngestErrors,
		)
	}
	return m
}

func (m *metrics) incTcpError() {
	m.tcpIngestErrors.Inc()
}

func (m *metrics) incGrpcError() {
	m.grpcIngestErrors.Inc()
}
