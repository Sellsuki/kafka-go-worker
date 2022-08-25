package example

import (
	"context"
	kafka_go_worker "github.com/sellsuki/kafka-go-worker"
	"github.com/sellsuki/kafka-go-worker/handler"
	"testing"
)

func Test_Example_10(t *testing.T) {
	initLogger()

	workerConfig.BatchSize = 1

	worker := kafka_go_worker.NewKafkaWorker(workerConfig,
		handler.WithAtLeastOnceCommitter,
		handler.WithSerialWorker(demoWorker, false, handler.WithRecovery),
	)

	// Run util context get cancelled
	worker.Start(context.Background())
}
