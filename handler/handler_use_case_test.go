package handler

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"log"
	"math/rand"
	"testing"
)

const commitMessagesMethodName = "CommitMessages"
const workerProcessorMethodName = "Processor"

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

func TestContext_UseCase_Simple(t *testing.T) {
	m1 := newHandlerMock(1)
	mWorker := newHandlerMock(4)
	mockWorker := newWorkerMock()

	c := Context{
		ctx:      context.Background(),
		Consumer: newKafkaConsumerMock(),
		handlers: []Handler{
			m1.Handle,
			WithAtLeastOnceCommitter,
			WithSerialWorker(mockWorker.Processor, false, mWorker.Handle),
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

	mockWorker := newWorkerMock()
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
			WithSerialWorker(mockWorker.Processor, false, mWorker.Handle),
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

	mockWorker := newWorkerMock()
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
			WithSerialWorker(mockWorker.Processor, false, mWorker.Handle),
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

func TestContext_UseCase_Concurrent_Key_With_Partition_Committer(t *testing.T) {
	m1 := newHandlerMock(1)
	m2 := newHandlerMock(3)
	mWorker := newHandlerMock(6)

	mockWorker := newWorkerMock()
	cm := newKafkaConsumerMock()

	msgs := genMessages(
		mockMessage{1, 1, "foo"},
		mockMessage{2, 1, "bar"},
		mockMessage{1, 2, "foo"},
		mockMessage{2, 2, "bar"},
		mockMessage{2, 3, "baz"},
		mockMessage{1, 3, "foo"},
	)
	c := Context{
		ctx:      context.Background(),
		Consumer: cm,
		handlers: []Handler{
			m1.Handle,
			WithForkByPartition,
			WithAtLeastOnceCommitter,
			WithForkByKey,
			m2.Handle,
			WithSerialWorker(mockWorker.Processor, false, mWorker.Handle),
		},
		Messages: msgs,
	}

	assert.NoError(t, c.Start())

	m1.AssertNumberOfCalls(t, handlerMockMethod, m1.expectCallCount)
	m2.AssertNumberOfCalls(t, handlerMockMethod, m2.expectCallCount)
	mWorker.AssertNumberOfCalls(t, handlerMockMethod, mWorker.expectCallCount)

	cm.AssertCalled(t, commitMessagesMethodName, mock.Anything, []kafka.Message{
		msgs[0], msgs[2], msgs[5],
	})
	cm.AssertCalled(t, commitMessagesMethodName, mock.Anything, []kafka.Message{
		msgs[1], msgs[3], msgs[4],
	})
	cm.AssertNumberOfCalls(t, commitMessagesMethodName, 2)
}
