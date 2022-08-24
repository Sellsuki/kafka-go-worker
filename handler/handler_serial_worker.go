package handler

import (
	"fmt"
	"github.com/segmentio/kafka-go"
)

// WithSerialWorker is a handler that process messages in sequential
func WithSerialWorker(p Processor, stopOnError bool, handlers ...Handler) Handler {
	return func(c *Context) error {
		errs := make([]error, 0, len(c.Messages))
		for _, msg := range c.Messages {

			ctx := NewContext(
				c.Context(),
				append(handlers, workerHandler(p, msg)),
				c.Consumer,
				[]kafka.Message{msg},
			)

			err := ctx.Start()
			if err != nil {
				if stopOnError {
					return err
				} else {
					errs = append(errs, err)
				}
			}
		}

		if len(errs) > 0 {
			return fmt.Errorf("%d errors occurred, errors: %v", len(errs), errs)
		}

		return c.Next()
	}
}
