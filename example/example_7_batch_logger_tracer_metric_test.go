package example

import (
	"context"
	"fmt"
	kafka_go_worker "github.com/sellsuki/kafka-go-worker"
	"github.com/sellsuki/kafka-go-worker/handler"
	"testing"
)

func Test_Example_7(t *testing.T) {
	initLogger()
	initTracer()

	worker := kafka_go_worker.NewKafkaWorker(workerConfig,
		handler.WithLoggerZap("batch_process", workerConfig.WorkerName, workerConfig.TopicName),
		handler.WithMetricPrometheus(
			"batch_Test_Example_7",
			prom, workerConfig.WorkerName,
			workerConfig.TopicName,
			workerConfig.BatchSize,
		),
		handler.WithTracerOtel(
			"kafka_consumer_batch",
			"kafka_consumer_worker_example_7",
			workerConfig.WorkerName,
			false,
		),
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
				"kafka_consumer_worker_example_7_worker",
				workerConfig.WorkerName,
				true,
			),
		),
	)

	// Run util context get cancelled
	worker.Start(context.Background())
}
