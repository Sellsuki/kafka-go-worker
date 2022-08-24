package handler

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"time"
)

const callOrderContextKey = "call_order"

type KafkaConsumerMock struct {
	mock.Mock
}

func (m *KafkaConsumerMock) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	return nil
}

func (m *KafkaConsumerMock) Stats() kafka.ReaderStats {
	return kafka.ReaderStats{}
}

func (m *KafkaConsumerMock) Config() kafka.ReaderConfig {
	return kafka.ReaderConfig{}
}

type handlerMock struct {
	mutex           *sync.Mutex
	expectCallCount int
	calledCount     int
}

func (m *handlerMock) reset() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.calledCount = 0
}

func (m *handlerMock) Handle(c *Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.calledCount++

	return c.Next()
}

func NewHandlerMock(expectCall int) *handlerMock {
	h := &handlerMock{
		mutex:           &sync.Mutex{},
		expectCallCount: expectCall,
		calledCount:     0,
	}

	return h
}

func getHandlers(hm []*handlerMock) []Handler {
	h := make([]Handler, len(hm))
	for i, m := range hm {
		if m != nil {
			h[i] = m.Handle
		}
	}

	return h
}

type mockMessage struct {
	partition int
	offset    int64
	key       string
}

func genMessages(msgs ...mockMessage) []kafka.Message {
	messages := make([]kafka.Message, 0, len(msgs))

	for _, m := range msgs {
		messages = append(messages, kafka.Message{
			Topic:     "test",
			Partition: m.partition,
			Key:       []byte(m.key),
			Offset:    m.offset,
			Headers:   nil,
			Value:     []byte("bar"),
			Time:      time.Now(),
		})
	}

	return messages
}

func TestContext_Start(t *testing.T) {
	tests := []struct {
		name         string
		wantErr      assert.ErrorAssertionFunc
		handlerMocks []*handlerMock
		messages     []kafka.Message
	}{
		{
			name:         "no handlers",
			messages:     genMessages(mockMessage{1, 1, ""}),
			handlerMocks: nil,
			wantErr:      assert.Error,
		},
		{
			name:         "1 handlers",
			messages:     genMessages(mockMessage{1, 1, ""}),
			handlerMocks: []*handlerMock{NewHandlerMock(1)},
			wantErr:      assert.NoError,
		},
		{
			name:     "3 handlers",
			messages: genMessages(mockMessage{1, 1, ""}),
			handlerMocks: []*handlerMock{
				NewHandlerMock(1),
				NewHandlerMock(1),
				NewHandlerMock(1),
			},
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Context{
				ctx:        context.Background(),
				handlerIdx: 0,
				handlers:   getHandlers(tt.handlerMocks),
				Messages:   tt.messages,
			}

			tt.wantErr(t, c.Start(), fmt.Sprintf("Start(%v)", c))

			for _, hm := range tt.handlerMocks {
				assert.Equal(t, hm.calledCount, hm.expectCallCount)
			}
		})
	}
}
