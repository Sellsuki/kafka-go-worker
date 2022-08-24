package handler

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWithForkByPartition(t *testing.T) {
	tests := []struct {
		name         string
		handlerMocks []*handlerMock
		messages     []kafka.Message
		wantErr      assert.ErrorAssertionFunc
	}{
		{
			name: "same partition",
			handlerMocks: []*handlerMock{
				newHandlerMock(1),
				nil,
				newHandlerMock(1),
			},
			messages: genMessages(
				mockMessage{1, 1, "foo"},
				mockMessage{1, 2, "foo"},
				mockMessage{1, 3, "foo"},
			),
			wantErr: assert.NoError,
		},
		{
			name: "3 partitions",
			handlerMocks: []*handlerMock{
				newHandlerMock(1), // before fork
				nil,
				newHandlerMock(3), // after fork
			},
			messages: genMessages(
				mockMessage{1, 1, "foo"},
				mockMessage{1, 2, "foo"},
				mockMessage{2, 3, "foo"},
				mockMessage{1, 4, "foo"},
				mockMessage{3, 5, "foo"},
				mockMessage{1, 6, "foo"},
			),
			wantErr: assert.NoError,
		},
		{
			name: "redundant",
			handlerMocks: []*handlerMock{
				newHandlerMock(1),
				nil,
				newHandlerMock(3),
				nil,
				newHandlerMock(3),
			},
			messages: genMessages(
				mockMessage{1, 1, "foo"},
				mockMessage{1, 2, "foo"},
				mockMessage{2, 3, "bar"},
				mockMessage{1, 4, "foo"},
				mockMessage{3, 5, "foo"},
				mockMessage{1, 6, "foo"},
			),
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hs := getHandlers(tt.handlerMocks)
			for i, h := range hs {
				if h == nil {
					hs[i] = WithForkByPartition
				}
			}

			c := Context{
				ctx:        context.Background(),
				handlerIdx: 0,
				handlers:   hs,
				Messages:   tt.messages,
			}

			tt.wantErr(t, c.Start(), fmt.Sprintf("WithForkByPartition(%v)", c))

			for _, hm := range tt.handlerMocks {
				if hm != nil {
					assert.Equal(t, hm.expectCallCount, hm.calledCount)
				}
			}
		})
	}
}
