package example

import (
	"context"
	kafka_consumer_worker "github.com/sellsuki/kafka-go-worker"
	"github.com/sellsuki/kafka-go-worker/handler"
	"testing"
)

func Test_Example_11(t *testing.T) {
	initLogger()
	initTracer()

	worker := kafka_consumer_worker.NewKafkaWorker(workerConfig,
		handler.WithTracerOtel(
			"kafka_consumer_batch",
			"kafka_consumer_worker_example_11",
			workerConfig.WorkerName,
			false,
		),
		handler.WithForkByPartition,
		handler.WithAtLeastOnceCommitter,
		handler.WithRejectPartitionOnFailed(),
		handler.WithSerialWorker(demoWorker, true,
			handler.WithRecovery,
			handler.WithTracerOtel(
				"kafka_consumer_worker",
				"kafka_consumer_worker_example_11_worker",
				workerConfig.WorkerName,
				true,
			),
		),
	)

	// Run util context get cancelled
	worker.Start(context.Background())
}
