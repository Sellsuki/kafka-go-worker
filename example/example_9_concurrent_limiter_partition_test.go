package example

import (
	"context"
	kafka_go_worker "github.com/sellsuki/kafka-go-worker"
	"github.com/sellsuki/kafka-go-worker/handler"
	"testing"
)

func Test_Example_9(t *testing.T) {
	initLogger()
	initTracer()

	worker := kafka_go_worker.NewKafkaWorker(workerConfig,
		handler.WithTracerOtel(
			"kafka_consumer_batch",
			"kafka_consumer_worker_example_9",
			workerConfig.WorkerName,
			false,
		),
		handler.WithForkByPartition,
		handler.WithAtLeastOnceCommitter,
		handler.WithConcurrentLimiter(1),
		handler.WithForkByKey,
		handler.WithSerialWorker(demoWorker, false,
			handler.WithRecovery,
			handler.WithConcurrentLimiter(3),
			handler.WithTracerOtel(
				"kafka_consumer_worker",
				"kafka_consumer_worker_example_9_worker",
				workerConfig.WorkerName,
				true,
			),
		),
	)

	// Run util context get cancelled
	worker.Start(context.Background())
}
