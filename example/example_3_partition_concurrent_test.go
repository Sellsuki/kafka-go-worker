package example

import (
	"context"
	kafka_consumer_worker "github.com/sellsuki/kafka-go-worker"
	"github.com/sellsuki/kafka-go-worker/handler"
	"testing"
)

// Test_Example_3 Partition Concurrent
// Received message in batch, fork each partition into separate thread
// Each partition will process message in serial (ordered), and commit once per partition
// Use case similar to Example 1, but have better process speed, due to concurrency
func Test_Example_3(t *testing.T) {
	initLogger()
	worker := kafka_consumer_worker.NewKafkaWorker(workerConfig,
		handler.WithForkByPartition,
		handler.WithAtLeastOnceCommitter,
		handler.WithSerialWorker(demoWorker, false, handler.WithRecovery),
	)

	worker.Start(context.Background())
}
