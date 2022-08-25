package example

import (
	"context"
	"fmt"
	kafka_consumer_worker "kafka-go-worker"
	"kafka-go-worker/handler"
	"testing"
)

func Test_Example_6(t *testing.T) {
	initLogger()
	initTracer()

	worker := kafka_consumer_worker.NewKafkaWorker(workerConfig,
		handler.WithAtLeastOnceCommitter,
		handler.WithSerialWorker(demoWorker, false,
			handler.WithRecovery,
			handler.WithLoggerZap("worker_process", workerConfig.WorkerName, workerConfig.TopicName),
			handler.WithMetricPrometheus(
				fmt.Sprintf("worker_%s", workerConfig.WorkerName),
				prom, workerConfig.WorkerName,
				workerConfig.TopicName,
				workerConfig.BatchSize,
			),
			handler.WithTracerOtel(
				"kafka_consumer_worker",
				"kafka_consumer_worker_example_6_worker",
				workerConfig.WorkerName,
				true,
			),
		),
	)

	// Run util context get cancelled
	worker.Start(context.Background())
}
