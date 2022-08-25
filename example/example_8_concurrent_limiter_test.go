package example

import (
	"context"
	kafka_consumer_worker "github.com/sellsuki/kafka-go-worker"
	"github.com/sellsuki/kafka-go-worker/handler"
	"testing"
)

func Test_Example_8(t *testing.T) {
	initLogger()
	initTracer()

	worker := kafka_consumer_worker.NewKafkaWorker(workerConfig,
		handler.WithTracerOtel(
			"kafka_consumer_batch",
			"kafka_consumer_worker_example_8",
			workerConfig.WorkerName,
			false,
		),
		handler.WithAtLeastOnceCommitter,
		handler.WithForkAll,
		handler.WithConcurrentLimiter(2),
		handler.WithSerialWorker(demoWorker, false,
			handler.WithRecovery,
			handler.WithTracerOtel(
				"kafka_consumer_worker",
				"kafka_consumer_worker_example_8_worker",
				workerConfig.WorkerName,
				true,
			),
		),
	)

	// Run util context get cancelled
	worker.Start(context.Background())
}
