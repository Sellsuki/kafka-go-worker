package handler

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWithForkAll(t *testing.T) {
	tests := []struct {
		name         string
		handlerMocks []*handlerMock
		messages     []kafka.Message
		wantErr      assert.ErrorAssertionFunc
	}{
		{
			name: "fork all",
			handlerMocks: []*handlerMock{
				newHandlerMock(1),
				nil,
				newHandlerMock(3),
			},
			messages: genMessages(
				mockMessage{1, 1, ""},
				mockMessage{1, 2, ""},
				mockMessage{1, 3, ""},
			),
			wantErr: assert.NoError,
		},
		{
			name: "fork multiple time",
			handlerMocks: []*handlerMock{
				newHandlerMock(1),
				nil,
				newHandlerMock(5),
				nil,
				newHandlerMock(5),
			},
			messages: genMessages(
				mockMessage{1, 1, ""},
				mockMessage{1, 2, ""},
				mockMessage{1, 3, ""},
				mockMessage{1, 4, ""},
				mockMessage{1, 5, ""},
			),
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hs := getHandlers(tt.handlerMocks)
			for i, h := range hs {
				if h == nil {
					hs[i] = WithForkAll
				}
			}

			c := Context{
				ctx:        context.Background(),
				handlerIdx: 0,
				handlers:   hs,
				Messages:   tt.messages,
			}

			tt.wantErr(t, c.Start(), fmt.Sprintf("WithForkAll(%v)", c))

			for _, hm := range tt.handlerMocks {
				if hm != nil {
					hm.AssertNumberOfCalls(t, handlerMockMethod, hm.expectCallCount)
				}
			}
		})
	}
}
