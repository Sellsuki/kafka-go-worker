package kafka_consumer

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Stats() kafka.ReaderStats
	Config() kafka.ReaderConfig
	FetchMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}
