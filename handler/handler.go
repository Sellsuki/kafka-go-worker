package handler

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"kafka-go-worker/kafka_consumer"
	"time"
)

var (
	ErrHandlersRequired = errors.New("at-least 1 handler required")
)

type Clock interface {
	Sleep(time.Duration)
}

type Context struct {
	ctx        context.Context
	handlerIdx int
	handlers   []Handler
	Messages   []kafka.Message
	Consumer   kafka_consumer.Consumer
	//Config     kafka_go_worker.WorkerConfig
}

func NewContext(ctx context.Context, handlers []Handler, consumer kafka_consumer.Consumer, messages []kafka.Message) *Context {
	return &Context{
		ctx:        ctx,
		handlerIdx: 0,
		handlers:   handlers,
		Messages:   messages,
		Consumer:   consumer,
	}
}

func (c *Context) Handlers() []Handler {
	return c.handlers
}

func (c *Context) Context() context.Context {
	return c.ctx
}

func (c *Context) ReplaceContext(ctx context.Context) {
	c.ctx = ctx
}

// Start context from beginning
func (c *Context) Start() (err error) {
	if len(c.handlers) == 0 {
		return ErrHandlersRequired
	}

	c.handlerIdx = 0
	return c.handlers[0](c)
}

// Next context from current handler
func (c *Context) Next() (err error) {
	c.handlerIdx++
	if c.handlerIdx < len(c.handlers) {
		err = c.handlers[c.handlerIdx](c)
	}

	return err
}

type Handler = func(*Context) error
