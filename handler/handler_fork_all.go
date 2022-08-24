package handler

import (
	"fmt"
	"github.com/segmentio/kafka-go"
)

// WithForkAll will fork every message into a new goroutine, and new context
func WithForkAll(c *Context) error {
	forkNumber := len(c.Messages)
	ch := make(chan error, forkNumber)
	for _, msg := range c.Messages {
		ctx := *c
		ctx.Messages = []kafka.Message{msg}

		go func(c *Context) {
			ch <- c.Next()
		}(&ctx)
	}

	var errs []error
	for i := 0; i < forkNumber; i++ {
		err := <-ch
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%d errors occurred, errors: %v", len(errs), errs)
	}

	return nil
}
