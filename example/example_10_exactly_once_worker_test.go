package example

import (
	"context"
	kafka_consumer_worker "kafka-go-worker"
	"kafka-go-worker/handler"
	"testing"
)

func Test_Example_10(t *testing.T) {
	initLogger()

	workerConfig.BatchSize = 1

	worker := kafka_consumer_worker.NewKafkaWorker(workerConfig,
		handler.WithAtLeastOnceCommitter,
		handler.WithSerialWorker(demoWorker, false, handler.WithRecovery),
	)

	// Run util context get cancelled
	worker.Start(context.Background())
}
