package example

import (
	kafka_go_worker "github.com/sellsuki/kafka-go-worker"
	"github.com/sellsuki/kafka-go-worker/handler"
	"testing"
)

// Test_Example_2 Simple worker With Manager
// Similar to Example1 But included graceful shutdown, and readiness probe
func Test_Example_2(t *testing.T) {
	initLogger()
	worker1 := kafka_go_worker.NewKafkaWorker(workerConfig,
		handler.WithAtLeastOnceCommitter,
		handler.WithSerialWorker(demoWorker, false, handler.WithRecovery),
	)
	worker2 := kafka_go_worker.NewKafkaWorker(workerConfig,
		handler.WithAtLeastOnceCommitter,
		handler.WithSerialWorker(demoWorker, false, handler.WithRecovery),
	)

	manager := kafka_go_worker.NewWorkerManager(worker1, worker2)

	// Workers will run util get SIGINT, or SIGTERM
	manager.Start()
}
