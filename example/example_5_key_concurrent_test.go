package example

import (
	"context"
	kafka_consumer_worker "kafka-go-worker"
	"kafka-go-worker/handler"
	"testing"
)

func Test_Example_5(t *testing.T) {
	initLogger()

	worker := kafka_consumer_worker.NewKafkaWorker(workerConfig,
		handler.WithForkByPartition,
		handler.WithAtLeastOnceCommitter,
		handler.WithForkByKey,
		handler.WithSerialWorker(demoWorker, false, handler.WithRecovery),
	)

	worker.Start(context.Background())
}
