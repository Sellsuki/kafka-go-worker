package handler

import (
	"fmt"
	"go.uber.org/zap"
	"strings"
	"time"
)

// HandlerLogger is a handler that logs the message and error
func withLogger(msg, workerName, topicName string) Handler {
	return func(c *Context) error {
		start := time.Now()

		err := c.Next()

		msgKeys, msgPartition := getLoggerFieldMessagePartitionOffsetAndKeys(c)
		zap.L().Info(msg,
			zap.Duration("duration", time.Since(start)),
			zap.String("worker_name", workerName),
			zap.Int("message_count", len(c.Messages)),
			zap.String("topic", topicName),
			zap.Error(err),
			msgKeys,
			msgPartition,
		)

		return err
	}
}

func getLoggerFieldMessagePartitionOffsetAndKeys(c *Context) (zap.Field, zap.Field) {
	messageKeys := make([]string, len(c.Messages))
	messagePartitionOffset := make([]string, len(c.Messages))
	for i, message := range c.Messages {
		messageKeys[i] = string(message.Key)
		messagePartitionOffset[i] = fmt.Sprintf("%d:%d", message.Partition, message.Offset)
	}

	return zap.String("message_keys", strings.Join(messageKeys, ", ")),
		zap.String("message_partition_offset", strings.Join(messagePartitionOffset, ", "))
}
