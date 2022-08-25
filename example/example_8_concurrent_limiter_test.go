package example

import (
	"context"
	kafka_consumer_worker "kafka-go-worker"
	"kafka-go-worker/handler"
	"testing"
)

func Test_Example_8(t *testing.T) {
	initLogger()
	worker := kafka_consumer_worker.NewKafkaWorker(workerConfig,
		handler.WithAtLeastOnceCommitter,
		handler.WithForkAll,
		handler.WithConcurrentLimiter(2),
		handler.WithSerialWorker(demoWorker, false, handler.WithRecovery),
	)

	// Run util context get cancelled
	worker.Start(context.Background())
}
