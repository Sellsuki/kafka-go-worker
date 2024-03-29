package kafka_go_worker

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type KafkaWorkerManager struct {
	workers []*KafkaWorker
}

// Start all workers
func (k KafkaWorkerManager) Start(wg *sync.WaitGroup) []error {
	if wg == nil {
		wg = &sync.WaitGroup{}
	}
	ctx, cancel := context.WithCancel(context.Background())

	// Wait for OS SIGINT/SIGTERM then gracefully shutdown the workers
	{
		exit := make(chan os.Signal, 1)
		signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-exit
			cancel()
		}()
	}

	errs := make([]error, len(k.workers))
	for i, worker := range k.workers {
		wg.Add(1)
		go func(worker *KafkaWorker, i int) {
			defer wg.Done()
			errs[i] = worker.Start(ctx)
		}(worker, i)
	}

	wg.Wait()

	return errs
}

func (k KafkaWorkerManager) Health() error {
	for _, worker := range k.workers {
		if err := worker.Health(); err != nil {
			return fmt.Errorf("kafka worker: %s, error: %w", worker.config.WorkerName, err)
		}
	}

	return nil
}

func NewWorkerManager(workers ...*KafkaWorker) *KafkaWorkerManager {
	return &KafkaWorkerManager{
		workers: workers,
	}
}
