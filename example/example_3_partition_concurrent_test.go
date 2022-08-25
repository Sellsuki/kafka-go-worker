package example

import (
	"context"
	kafka_consumer_worker "kafka-go-worker"
	"kafka-go-worker/handler"
	"testing"
)

// Test_Example_3 Partition Concurrent
// Received message in batch, fork each partition into separate thread
// Each partition will message in serial (ordered), and get commit separately
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
