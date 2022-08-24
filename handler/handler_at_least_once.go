package handler

import (
	"context"
	"time"
)

// WithAtLeastOnceCommitter will commit after all message are processed successfully
func WithAtLeastOnceCommitter(c *Context) error {
	cErr := c.Next()

	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Second)
	defer cancel()

	err := c.Consumer.CommitMessages(ctx, c.Messages...)
	if err != nil {
		return err
	}

	return cErr
}
