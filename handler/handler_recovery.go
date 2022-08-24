package handler

import (
	"fmt"
)

// WithRecovery prevent program from crashing in case of panic
func WithRecovery(c *Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			//zap.L().Error("kafka Consumer panic",
			//	zap.String("worker_name", c.Config.WorkerName),
			//	zap.String("panic", fmt.Sprintf("%v", r)),
			//	zap.Int("message_count", len(c.Messages)),
			//	zap.String("topic", c.Config.TopicName),
			//)

			switch v := r.(type) {
			case error:
				err = v
			default:
				err = fmt.Errorf("panic: %v", v)
			}
		}
	}()

	return c.Next()
}
