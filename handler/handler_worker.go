package handler

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

type Processor func(ctx context.Context, message kafka.Message) error

func workerHandler(p Processor, msg kafka.Message) Handler {
	return func(c *Context) error {
		if err := p(c.Context(), msg); err != nil {
			return fmt.Errorf("worker error occurred, error: %v, topic %s, parition %d, offset %d",
				err, msg.Topic, msg.Partition, msg.Offset)
		}
		return c.Next()
	}
}
