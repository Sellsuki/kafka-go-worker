package handler

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
	"log"
	"math/rand"
)

const commitMessagesMethodName = "CommitMessages"
const workerProcessorMethodName = "Processor"

type kafkaConsumerMock struct {
	mock.Mock
}

func (m *kafkaConsumerMock) FetchMessage(_ context.Context) (kafka.Message, error) {
	return genMessages(mockMessage{rand.Int(), rand.Int63(), "", ""})[0], nil
}

func (m *kafkaConsumerMock) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.Called(ctx, msgs)
	return nil
}

func (m *kafkaConsumerMock) Stats() kafka.ReaderStats {
	return kafka.ReaderStats{}
}

func (m *kafkaConsumerMock) Config() kafka.ReaderConfig {
	return kafka.ReaderConfig{}
}

type workerMock struct {
	mock.Mock
}

func (w *workerMock) Processor(ctx context.Context, msg kafka.Message) error {
	res := w.Called(ctx, msg)
	switch v := res[0].(type) {
	case error:
		return v
	case nil:
		return nil
	default:
		log.Printf("unexpected result of: %+v", v)
		return nil
	}

}

func newKafkaConsumerMock() *kafkaConsumerMock {
	k := new(kafkaConsumerMock)

	k.On(commitMessagesMethodName, mock.Anything, mock.Anything).Return(nil)

	return k
}

func newWorkerMock() *workerMock {
	w := new(workerMock)

	w.On(workerProcessorMethodName, mock.Anything, mock.Anything).Return(nil)

	return w
}
