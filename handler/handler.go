package handler

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	kafka_consumer_worker "kafka-go-worker"
	"time"
)

var (
	ErrHandlersRequired = errors.New("at-least 1 handler required")
)

type Consumer interface {
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Stats() kafka.ReaderStats
	Config() kafka.ReaderConfig
}

type Clock interface {
	Sleep(time.Duration)
}

type Context struct {
	ctx        context.Context
	handlerIdx int
	handlers   []Handler
	Messages   []kafka.Message
	Consumer   Consumer
	Config     kafka_consumer_worker.WorkerConfig
}

func NewContext(ctx context.Context, handlers []Handler, consumer Consumer, messages []kafka.Message) *Context {
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
