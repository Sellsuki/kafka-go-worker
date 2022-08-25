package handler

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"math/rand"
	"testing"
)

const commitMessagesMethodName = "CommitMessages"

type kafkaConsumerMock struct {
	mock.Mock
}

func (m *kafkaConsumerMock) FetchMessage(_ context.Context) (kafka.Message, error) {
	return genMessages(mockMessage{rand.Int(), rand.Int63(), ""})[0], nil
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

func newKafkaConsumerMock() *kafkaConsumerMock {
	k := new(kafkaConsumerMock)

	k.On(commitMessagesMethodName, mock.Anything, mock.Anything).Return(nil)

	return k
}

func mockWorker(_ context.Context, _ kafka.Message) error {
	return nil
}

func TestContext_UseCase_Simple(t *testing.T) {
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
			mockMessage{1, 2, "foo"},
			mockMessage{1, 3, "foo"},
			mockMessage{1, 4, "foo"},
		),
	}

	assert.NoError(t, c.Start())

	m1.AssertNumberOfCalls(t, handlerMockMethod, m1.expectCallCount)
	mWorker.AssertNumberOfCalls(t, handlerMockMethod, mWorker.expectCallCount)
}

func TestContext_UseCase_Concurrent_Partition(t *testing.T) {
	m1 := newHandlerMock(1)
	m2 := newHandlerMock(2)
	mWorker := newHandlerMock(6)

	cm := newKafkaConsumerMock()

	msgs := genMessages(
		mockMessage{1, 1, "foo"},
		mockMessage{2, 1, "bar"},
		mockMessage{1, 2, "foo"},
		mockMessage{1, 3, "foo"},
		mockMessage{2, 2, "baz"},
		mockMessage{1, 4, "foo"},
	)

	c := Context{
		ctx:      context.Background(),
		Consumer: cm,
		handlers: []Handler{
			m1.Handle,
			WithForkByPartition,
			WithAtLeastOnceCommitter,
			m2.Handle,
			WithSerialWorker(mockWorker, false, mWorker.Handle),
		},
		Messages: msgs,
	}

	assert.NoError(t, c.Start())

	m1.AssertNumberOfCalls(t, handlerMockMethod, m1.expectCallCount)
	m2.AssertNumberOfCalls(t, handlerMockMethod, m2.expectCallCount)
	mWorker.AssertNumberOfCalls(t, handlerMockMethod, mWorker.expectCallCount)

	cm.AssertCalled(t, commitMessagesMethodName, mock.Anything, []kafka.Message{
		msgs[0], msgs[2], msgs[3], msgs[5],
	})
	cm.AssertCalled(t, commitMessagesMethodName, mock.Anything, []kafka.Message{
		msgs[1], msgs[4],
	})
	cm.AssertNumberOfCalls(t, commitMessagesMethodName, m2.expectCallCount)
}

func TestContext_UseCase_Concurrent_Key(t *testing.T) {
	m1 := newHandlerMock(1)
	m2 := newHandlerMock(3)
	mWorker := newHandlerMock(6)

	cm := newKafkaConsumerMock()

	msgs := genMessages(
		mockMessage{1, 1, "foo"},
		mockMessage{2, 1, "bar"},
		mockMessage{1, 2, "foo"},
		mockMessage{1, 3, "bar"},
		mockMessage{2, 2, "baz"},
		mockMessage{1, 4, "foo"},
	)
	c := Context{
		ctx:      context.Background(),
		Consumer: cm,
		handlers: []Handler{
			m1.Handle,
			WithAtLeastOnceCommitter,
			WithForkByKey,
			m2.Handle,
			WithSerialWorker(mockWorker, false, mWorker.Handle),
		},
		Messages: msgs,
	}

	assert.NoError(t, c.Start())

	m1.AssertNumberOfCalls(t, handlerMockMethod, m1.expectCallCount)
	m2.AssertNumberOfCalls(t, handlerMockMethod, m2.expectCallCount)
	mWorker.AssertNumberOfCalls(t, handlerMockMethod, mWorker.expectCallCount)

	cm.AssertCalled(t, commitMessagesMethodName, mock.Anything, msgs)
	cm.AssertNumberOfCalls(t, commitMessagesMethodName, 1)
}
