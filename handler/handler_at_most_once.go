package handler

import (
	"context"
	"time"
)

// WithAtMostOnceCommitter will commit first then process the messages
func WithAtMostOnceCommitter(c *Context) error {
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Second)
	defer cancel()

	err := c.Consumer.CommitMessages(ctx, c.Messages...)
	if err != nil {
		return err
	}

	return c.Next()
}
