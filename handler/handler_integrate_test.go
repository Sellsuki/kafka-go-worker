package handler

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type kafkaConsumerMock struct {
	mock.Mock
}

func (m *kafkaConsumerMock) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	return nil
}

func (m *kafkaConsumerMock) Stats() kafka.ReaderStats {
	return kafka.ReaderStats{}
}

func (m *kafkaConsumerMock) Config() kafka.ReaderConfig {
	return kafka.ReaderConfig{}
}

func newKafkaConsumerMock() *kafkaConsumerMock {
	k := new(kafkaConsumerMock)

	return k
}

func mockWorker(_ context.Context, _ kafka.Message) error {
	return nil
}

func TestContext_Simple_UseCase(t *testing.T) {
	m1 := newHandlerMock(1)
	mWorker := newHandlerMock(4)

	c := Context{
		ctx:      context.Background(),
		Consumer: newKafkaConsumerMock(),
		handlers: []Handler{
			m1.Handle,
			WithAtLeastOnceCommitter,
			WithSerialWorker(mockWorker, false, mWorker.Handle),
		},
		Messages: genMessages(
			mockMessage{1, 1, "foo"},
			mockMessage{1, 1, "foo"},
			mockMessage{1, 1, "foo"},
			mockMessage{1, 1, "foo"},
		),
	}

	assert.NoError(t, c.Start())
}
