package handler

import (
	"github.com/segmentio/kafka-go"
	"sync"
)

// WithRejectPartitionOnFailed remove failed partition from context, the failed partition still get pulled but won't process or commit
// attention: DO NOT USE AFTER FORK, OR IN WORKER'S HANDLER
// the WithRejectPartitionOnFailed must use after WithAtLeastOnceCommitter and WithForkByPartition
// e.g. NewKafkaWorker(config, WithRecovery, WithForkByPartition, WithAtLeastOnceCommitter, WithRejectPartitionOnFailed(), withSerialWorker(demoWorker))
func WithRejectPartitionOnFailed() Handler {
	lock := sync.Mutex{}
	rejectedPartitions := map[int]bool{}

	return func(c *Context) error {
		acceptedMsg := make([]kafka.Message, 0, len(c.Messages))

		for _, msg := range c.Messages {
			if rejected, ok := rejectedPartitions[msg.Partition]; ok && rejected {
				//zap.L().Warn("Rejected message from partition",
				//	zap.String("topic", msg.Topic),
				//	zap.Int("partition", msg.Partition),
				//	zap.Int64("offset", msg.Offset),
				//	zap.ByteString("key", msg.Key),
				//)
			} else {
				acceptedMsg = append(acceptedMsg, msg)
			}
		}

		c.Messages = acceptedMsg

		err := c.Next()
		if err == nil {
			return nil
		}

		lock.Lock()
		defer lock.Unlock()

		for _, msg := range c.Messages {
			rejectedPartitions[msg.Partition] = true
		}

		c.Messages = nil

		return err
	}
}
