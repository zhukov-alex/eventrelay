package output

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type KafkaBroker struct {
	producer sarama.AsyncProducer
	cfg      *KafkaConfig
	logger   *zap.Logger
}

func NewKafkaBroker(logger *zap.Logger, cfg *KafkaConfig) (*KafkaBroker, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Flush.Messages = cfg.FlushMessages
	saramaCfg.Producer.Flush.Frequency = cfg.FlushFrequency
	saramaCfg.ChannelBufferSize = cfg.ChannelBufferSize
	saramaCfg.Producer.RequiredAcks = sarama.WaitForLocal
	saramaCfg.Producer.Idempotent = false

	producer, err := sarama.NewAsyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		logger.Error("failed to create Kafka producer", zap.Error(err))
		return nil, err
	}

	logger.Info("kafka producer initialized",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("topic", cfg.Topic),
	)

	return &KafkaBroker{
		producer: producer,
		cfg:      cfg,
		logger:   logger,
	}, nil
}

func (k *KafkaBroker) Send(_ context.Context, _ OutputMessage, _ string) error {
	return fmt.Errorf("KafkaBroker.Send is not implemented")
}

func (k *KafkaBroker) SendBatch(ctx context.Context, batch OutputBatchMessage) error {
	logger := k.logger.With(zap.String("method", "SendBatch"))

	expectedIDs := make(map[string]struct{})
	remaining := make(map[string]struct{})
	for _, msg := range batch.Batch {
		expectedIDs[msg.ID] = struct{}{}
		remaining[msg.ID] = struct{}{}
	}

	done := make(chan struct{})
	errChan := make(chan error, 1)

	go func() {
		for {
			select {
			case msg := <-k.producer.Successes():
				meta, ok := msg.Metadata.(OutputMessage)
				if !ok {
					logger.Warn("kafka ack metadata type mismatch")
					continue
				}

				if _, ok := expectedIDs[meta.ID]; !ok {
					logger.Warn("kafka acked unknown message ID", zap.String("id", meta.ID))
					continue
				}

				if _, still := remaining[meta.ID]; still {
					delete(remaining, meta.ID)
				}

				if len(remaining) == 0 {
					close(done)
					return
				}

			case err := <-k.producer.Errors():
				logger.Error("kafka send error", zap.Error(err.Err))
				errChan <- err.Err
				return

			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}
		}
	}()

	for _, m := range batch.Batch {
		msg := &sarama.ProducerMessage{
			Topic:    k.cfg.Topic,
			Value:    sarama.ByteEncoder(m.Payload),
			Key:      sarama.StringEncoder(batch.WorkerID),
			Metadata: m,
		}

		select {
		case k.producer.Input() <- msg:
		case <-ctx.Done():
			logger.Warn("context cancelled while sending to kafka")
			return ctx.Err()
		case <-time.After(1 * time.Second):
			logger.Warn("timeout on Kafka input queue")
			return fmt.Errorf("timeout on input queue")
		}
	}

	select {
	case <-done:
		return nil
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (k *KafkaBroker) Close(ctx context.Context) error {
	k.logger.Info("kafka producer shutting down...")
	done := make(chan struct{})

	go func() {
		err := k.producer.Close()
		if err != nil {
			k.logger.Warn("error while closing Kafka producer", zap.Error(err))
		}
		close(done)
	}()

	select {
	case <-done:
		k.logger.Info("kafka producer closed")
		return nil
	case <-ctx.Done():
		k.logger.Warn("kafka producer close timeout", zap.Error(ctx.Err()))
		return ctx.Err()
	}
}
