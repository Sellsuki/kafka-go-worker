package handler

import (
	"context"
	"time"
)

// WithAtLeastOnceCommitter will commit after all message are processed successfully
func WithAtLeastOnceCommitter(c *Context) error {
	err := c.Next()

	if len(c.Messages) > 0 {
		ctx, cancel := context.WithTimeout(c.Context(), 5*time.Second)
		defer cancel()

		err := c.Consumer.CommitMessages(ctx, c.Messages...)
		if err != nil {
			return err
		}
	}

	return err
}
