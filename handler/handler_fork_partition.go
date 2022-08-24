package handler

import (
	"fmt"
	"github.com/segmentio/kafka-go"
)

// WithForkByPartition will fork for each partition into a new goroutine, and new context
func WithForkByPartition(c *Context) error {
	msgGroup := map[int][]kafka.Message{}

	for _, msg := range c.Messages {
		if _, ok := msgGroup[msg.Partition]; !ok {
			msgGroup[msg.Partition] = []kafka.Message{}
		}

		msgGroup[msg.Partition] = append(msgGroup[msg.Partition], msg)
	}

	forkNumber := len(msgGroup)

	ch := make(chan error, forkNumber)
	for _, msgs := range msgGroup {
		ctx := *c
		ctx.Messages = msgs

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
