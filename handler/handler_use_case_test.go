package handler

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestContext_UseCase_Simple(t *testing.T) {
	m1 := newHandlerMock(1)
	mWorker := newHandlerMock(4)
	mockWorker := newWorkerMock()

	c := Context{
		ctx:      context.Background(),
		Consumer: newKafkaConsumerMock(),
		handlers: []Handler{
			WithRecovery,
			m1.Handle,
			WithAtLeastOnceCommitter,
			WithSerialWorker(mockWorker.Processor, false, mWorker.Handle),
		},
		Messages: genMessages(
			mockMessage{1, 1, "foo", ""},
			mockMessage{1, 2, "foo", ""},
			mockMessage{1, 3, "foo", ""},
			mockMessage{1, 4, "foo", ""},
		),
	}

	mockWorker.On(workerProcessorMethodName, mock.Anything, mock.Anything).Return(nil)

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
		mockMessage{1, 1, "foo", ""},
		mockMessage{2, 1, "bar", ""},
		mockMessage{1, 2, "foo", ""},
		mockMessage{1, 3, "foo", ""},
		mockMessage{2, 2, "baz", ""},
		mockMessage{1, 4, "foo", ""},
	)

	c := Context{
		ctx:      context.Background(),
		Consumer: cm,
		handlers: []Handler{
			WithRecovery,
			m1.Handle,
			WithForkByPartition,
			WithAtLeastOnceCommitter,
			m2.Handle,
			WithSerialWorker(mockWorker.Processor, false, mWorker.Handle),
		},
		Messages: msgs,
	}

	mockWorker.On(workerProcessorMethodName, mock.Anything, mock.Anything).Return(nil)

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
		mockMessage{1, 1, "foo", ""},
		mockMessage{2, 1, "bar", ""},
		mockMessage{1, 2, "foo", ""},
		mockMessage{1, 3, "bar", ""},
		mockMessage{2, 2, "baz", ""},
		mockMessage{1, 4, "foo", ""},
	)
	c := Context{
		ctx:      context.Background(),
		Consumer: cm,
		handlers: []Handler{
			WithRecovery,
			m1.Handle,
			WithAtLeastOnceCommitter,
			WithForkByKey,
			m2.Handle,
			WithSerialWorker(mockWorker.Processor, false, mWorker.Handle),
		},
		Messages: msgs,
	}

	mockWorker.On(workerProcessorMethodName, mock.Anything, mock.Anything).Return(nil)

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
		mockMessage{1, 1, "foo", ""},
		mockMessage{2, 1, "bar", ""},
		mockMessage{1, 2, "foo", ""},
		mockMessage{2, 2, "bar", ""},
		mockMessage{2, 3, "baz", ""},
		mockMessage{1, 3, "foo", ""},
	)
	c := Context{
		ctx:      context.Background(),
		Consumer: cm,
		handlers: []Handler{
			WithRecovery,
			m1.Handle,
			WithForkByPartition,
			WithAtLeastOnceCommitter,
			WithForkByKey,
			m2.Handle,
			WithSerialWorker(mockWorker.Processor, false, mWorker.Handle),
		},
		Messages: msgs,
	}

	mockWorker.On(workerProcessorMethodName, mock.Anything, mock.Anything).Return(nil)

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

func TestContext_UseCase_Halt_Failed_Partition(t *testing.T) {
	m1 := newHandlerMock(1)
	m2 := newHandlerMock(2)
	mWorker := newHandlerMock(5)

	mockWorker := newWorkerMock()
	cm := newKafkaConsumerMock()

	msgs := genMessages(
		mockMessage{1, 1, "foo", "1"},
		mockMessage{2, 1, "bar", "2"},
		mockMessage{1, 2, "foo", "3"},
		mockMessage{2, 2, "bar", "4"},
		mockMessage{2, 3, "baz", "5"},
		mockMessage{1, 3, "foo", "6"},
	)

	c := Context{
		ctx:      context.Background(),
		Consumer: cm,
		handlers: []Handler{
			WithRecovery,
			m1.Handle,
			WithForkByPartition,
			WithAtLeastOnceCommitter,
			WithRejectPartitionOnFailed(),
			m2.Handle,
			WithSerialWorker(mockWorker.Processor, true, mWorker.Handle),
		},
		Messages: msgs,
	}

	// worker that get msgs[3] will fail
	mockWorker.On(workerProcessorMethodName, mock.Anything, msgs[3]).Return(ErrWorkerMock1)
	mockWorker.On(workerProcessorMethodName, mock.Anything, mock.Anything).Return(nil)

	assert.Error(t, c.Start())

	m1.AssertNumberOfCalls(t, handlerMockMethod, m1.expectCallCount)
	m2.AssertNumberOfCalls(t, handlerMockMethod, m2.expectCallCount)
	mWorker.AssertNumberOfCalls(t, handlerMockMethod, mWorker.expectCallCount)

	cm.AssertCalled(t, commitMessagesMethodName, mock.Anything, []kafka.Message{
		msgs[0], msgs[2], msgs[5],
	})
	// partition 2 will get rejected and never get processed anymore
	cm.AssertNumberOfCalls(t, commitMessagesMethodName, 1)

	// iteration 2 AKA next pull
	msgs = genMessages(
		mockMessage{1, 5, "foo", "1"},
		mockMessage{2, 6, "bar", "2"},
	)

	c.Messages = msgs

	assert.NoError(t, c.Start())

	m1.AssertNumberOfCalls(t, handlerMockMethod, m1.expectCallCount+1)
	m2.AssertNumberOfCalls(t, handlerMockMethod, m2.expectCallCount*2)
	mWorker.AssertNumberOfCalls(t, handlerMockMethod, mWorker.expectCallCount+1)

	cm.AssertCalled(t, commitMessagesMethodName, mock.Anything, []kafka.Message{
		msgs[0],
	})
	cm.AssertNumberOfCalls(t, commitMessagesMethodName, 2)
}

func TestContext_UseCase_Fast_Parallel_Batch(t *testing.T) {
	m1 := newHandlerMock(1)
	m2 := newHandlerMock(6)
	mWorker := newHandlerMock(6)

	mockWorker := newWorkerMock()
	cm := newKafkaConsumerMock()

	msgs := genMessages(
		mockMessage{1, 1, "foo", ""},
		mockMessage{2, 1, "bar", ""},
		mockMessage{1, 2, "foo", ""},
		mockMessage{2, 2, "bar", ""},
		mockMessage{2, 3, "baz", ""},
		mockMessage{1, 3, "foo", ""},
	)
	c := Context{
		ctx:      context.Background(),
		Consumer: cm,
		handlers: []Handler{
			WithRecovery,
			m1.Handle,
			WithAtMostOnceCommitter,
			WithForkAll,
			m2.Handle,
			WithSerialWorker(mockWorker.Processor, false, mWorker.Handle),
		},
		Messages: msgs,
	}

	mockWorker.On(workerProcessorMethodName, mock.Anything, mock.Anything).Return(nil)

	assert.NoError(t, c.Start())

	m1.AssertNumberOfCalls(t, handlerMockMethod, m1.expectCallCount)
	m2.AssertNumberOfCalls(t, handlerMockMethod, m2.expectCallCount)
	mWorker.AssertNumberOfCalls(t, handlerMockMethod, mWorker.expectCallCount)

	cm.AssertCalled(t, commitMessagesMethodName, mock.Anything, msgs)
	cm.AssertNumberOfCalls(t, commitMessagesMethodName, 1)
}
