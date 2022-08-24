package handler

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWithForkByKey(t *testing.T) {
	tests := []struct {
		name         string
		handlerMocks []*handlerMock
		messages     []kafka.Message
		wantErr      assert.ErrorAssertionFunc
	}{
		{
			name: "same key",
			handlerMocks: []*handlerMock{
				NewHandlerMock(1),
				nil,
				NewHandlerMock(1),
			},
			messages: genMessages(
				mockMessage{1, 1, "foo"},
				mockMessage{1, 2, "foo"},
				mockMessage{1, 3, "foo"},
			),
			wantErr: assert.NoError,
		},
		{
			name: "2 keys",
			handlerMocks: []*handlerMock{
				NewHandlerMock(1),
				nil,
				NewHandlerMock(2),
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
		{
			name: "redundant",
			handlerMocks: []*handlerMock{
				NewHandlerMock(1),
				nil,
				NewHandlerMock(2),
				nil,
				NewHandlerMock(2),
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
		{
			name: "all different",
			handlerMocks: []*handlerMock{
				NewHandlerMock(1),
				nil,
				NewHandlerMock(6),
			},
			messages: genMessages(
				mockMessage{1, 1, "foo"},
				mockMessage{1, 2, "bar"},
				mockMessage{2, 3, "baz"},
				mockMessage{1, 4, "FOO"},
				mockMessage{3, 5, "Foo"},
				mockMessage{1, 6, "123"},
			),
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hs := getHandlers(tt.handlerMocks)
			for i, h := range hs {
				if h == nil {
					hs[i] = WithForkByKey
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
					assert.Equal(t, hm.expectCallCount, hm.calledCount)
				}
			}
		})
	}
}
