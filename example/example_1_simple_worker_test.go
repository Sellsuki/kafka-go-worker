package example

import (
	"context"
	kafka_go_worker "github.com/sellsuki/kafka-go-worker"
	"github.com/sellsuki/kafka-go-worker/handler"
	"testing"
)

// Test_Example_1 Simple worker
// Received message in batch, process message 1 by 1 until all messages processed
// then commit ALL message in batch
// if some messages failed it still got COMMITTED
// Use case generic kafka pipeline, need handle failed message later without blocking the stream
func Test_Example_1(t *testing.T) {
	initLogger()
	worker := kafka_go_worker.NewKafkaWorker(workerConfig,
		handler.WithAtLeastOnceCommitter,
		handler.WithSerialWorker(demoWorker, false, handler.WithRecovery),
	)

	// Run util context get cancelled
	worker.Start(context.Background())
}
