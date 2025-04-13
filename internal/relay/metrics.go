package relay

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	walWriteLatency   prometheus.Histogram
	kafkaBatchLatency prometheus.Histogram
}

func initMetrics(register bool) *metrics {
	m := &metrics{
		walWriteLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "eventrelay",
			Subsystem: "relay",
			Name:      "wal_write_latency_seconds",
			Help:      "Latency of writing a message to the WAL (with optional flush)",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
		}),
		kafkaBatchLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "eventrelay",
			Subsystem: "output",
			Name:      "kafka_batch_latency_seconds",
			Help:      "Latency of sending an entire Kafka message batch",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0},
		}),
	}

	if register {
		prometheus.MustRegister(
			m.walWriteLatency,
			m.kafkaBatchLatency,
		)
	}
	return m
}

func (m *metrics) kafkaBatchTimer() (stop func()) {
	timer := prometheus.NewTimer(m.kafkaBatchLatency)
	stop = func() {
		timer.ObserveDuration()
	}
	return
}
