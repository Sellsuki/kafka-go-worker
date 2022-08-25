package example

import (
	kafka_consumer_worker "kafka-go-worker"
	"kafka-go-worker/handler"
	"testing"
)

// Test_Example_2 Simple worker With Manager
// Similar to Example1 But included graceful shutdown, and readiness probe
func Test_Example_2(t *testing.T) {
	initLogger()
	worker1 := kafka_consumer_worker.NewKafkaWorker(workerConfig,
		handler.WithAtLeastOnceCommitter,
		handler.WithSerialWorker(demoWorker, false, handler.WithRecovery),
	)
	worker2 := kafka_consumer_worker.NewKafkaWorker(workerConfig,
		handler.WithAtLeastOnceCommitter,
		handler.WithSerialWorker(demoWorker, false, handler.WithRecovery),
	)

	manager := kafka_consumer_worker.NewWorkerManager(worker1, worker2)

	// Workers will run util get SIGINT, or SIGTERM
	manager.Start()
}
