package handler

import (
	"fmt"
	"github.com/segmentio/kafka-go"
)

// WithForkByKey will fork for each message key into a new goroutine, and new context
// attention: this will break topic partition order while processing
func WithForkByKey(c *Context) error {
	msgGroup := map[string][]kafka.Message{}

	for _, msg := range c.Messages {
		key := string(msg.Key)
		if _, ok := msgGroup[key]; !ok {
			msgGroup[key] = []kafka.Message{}
		}

		msgGroup[key] = append(msgGroup[key], msg)
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
