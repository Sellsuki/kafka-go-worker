package handler

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWithRecovery(t *testing.T) {
	m1 := newHandlerMock(1)
	m2 := newHandlerMock(1)

	c := Context{
		ctx:        context.Background(),
		handlerIdx: 0,
		handlers: []Handler{
			m1.Handle,
			WithRecovery,
			m2.Handle,
			func(c *Context) error {
				panic(ErrWorkerMock1)
			},
		},
		Messages: genMessages(mockMessage{1, 1, "", ""}),
	}

	assert.ErrorIs(t, c.Start(), ErrWorkerMock1)

	m1.AssertNumberOfCalls(t, handlerMockMethod, m1.expectCallCount)
	m2.AssertNumberOfCalls(t, handlerMockMethod, m2.expectCallCount)
}
