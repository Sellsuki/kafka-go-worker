package kafka_go_worker

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/segmentio/kafka-go"
	"github.com/sellsuki/kafka-go-worker/handler"
	"github.com/sellsuki/kafka-go-worker/kafka_consumer"
	"math"
	"time"
)

var (
	ErrWorkingAlreadyRunning = errors.New("working already running")
	ErrNoHandlersDefined     = errors.New("no handlers defined")
	ErrWorkerStopped         = errors.New("worker stopped")
)

type WorkerConfig struct {
	TopicName       string
	WorkerName      string
	KafkaBrokers    []string
	BatchSize       int
	MaxWait         time.Duration
	BackoffDelay    time.Duration
	MaxBackoffDelay time.Duration
	MaxProcessTime  time.Duration
	UseTLS          bool
	TLSSkipVerify   bool
}

func (w *WorkerConfig) SetDefault() {
	if w.BackoffDelay <= 0 {
		w.BackoffDelay = 1 * time.Second
	}

	if w.MaxBackoffDelay <= 0 {
		w.MaxBackoffDelay = 30 * time.Second
	}

	if w.MaxWait <= 0 {
		w.MaxWait = 1 * time.Second
	}

	if w.BatchSize <= 0 {
		w.BatchSize = 100
	}

	if w.MaxProcessTime <= 0 {
		w.MaxProcessTime = 30 * time.Second
	}
}

type KafkaWorker struct {
	consumer   kafka_consumer.Consumer
	running    bool
	error      error
	config     WorkerConfig
	handlers   []handler.Handler
	clock      handler.Clock
	errorCount uint64
}

func (k *KafkaWorker) Health() error {
	if !k.running {
		return ErrWorkerStopped
	}

	if k.error != nil {
		return k.error
	}

	return nil
}

// resetError
func (k *KafkaWorker) resetError() {
	k.error = nil
	k.errorCount = 0
}

func (k *KafkaWorker) handleErrorWithBackoff(err error) {
	if err == nil {
		return
	}

	k.error = err
	k.errorCount++

	// sleep exponentially every second it failed e.g. (1 => 1s, 2 => 4s, 3 => 9s, 4 => 16s, 5 => 25s, 6 => 36s, ...)
	sleepTime := time.Duration(math.Pow(2, float64(k.errorCount))) * k.config.BackoffDelay
	// max sleep time
	if sleepTime > k.config.MaxBackoffDelay {
		sleepTime = k.config.MaxBackoffDelay
	}

	if k.clock != nil {
		k.clock.Sleep(sleepTime)
	} else {
		time.Sleep(sleepTime)
	}
}

func (k *KafkaWorker) Start(ctx context.Context) error {
	if len(k.handlers) == 0 {
		return ErrNoHandlersDefined
	}

	if k.running {
		return ErrWorkingAlreadyRunning
	}
	k.running = true
	defer func() {
		k.running = false
	}()

	for {
		// Start until context is cancelled
		if err := ctx.Err(); err != nil {
			//zap.L().Info("kafka worker shutdown", zap.String("name", k.config.WorkerName))
			return k.consumer.Close()
		}

		messages, err := k.pull()
		if err != nil {
			k.handleErrorWithBackoff(err)
			continue
		}

		if len(messages) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		c := handler.NewContext(ctx, k.handlers, k.consumer, messages)

		//zap.L().Info("Kafka worker batch started", zap.String("name", k.config.WorkerName), zap.Int("batch_size", len(messages)))
		err = c.Start()
		if err != nil {
			k.handleErrorWithBackoff(err)
			continue
		}

		k.resetError()
	}
}

func (k *KafkaWorker) pull() ([]kafka.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), k.config.MaxWait)
	defer cancel()

	var messages []kafka.Message

	for {
		msg, err := k.consumer.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return messages, nil
			}

			//zap.L().Error("Error fetching message", zap.Error(err))
			return messages, err
		}

		messages = append(messages, msg)
		if len(messages) >= k.config.BatchSize {
			return messages, nil
		}
	}
}

func NewKafkaWorkerWithCustomer(workerConfig WorkerConfig, customer *kafka.Reader, handlers ...handler.Handler) *KafkaWorker {
	workerConfig.SetDefault()

	return &KafkaWorker{
		consumer: customer,
		running:  false,
		error:    nil,
		config:   workerConfig,
		handlers: handlers,
	}
}

func NewKafkaWorker(workerConfig WorkerConfig, handlers ...handler.Handler) *KafkaWorker {
	workerConfig.SetDefault()
	config := kafka.ReaderConfig{
		Brokers:                workerConfig.KafkaBrokers,
		GroupID:                workerConfig.WorkerName,
		Topic:                  workerConfig.TopicName,
		QueueCapacity:          workerConfig.BatchSize,
		MaxWait:                workerConfig.MaxWait,
		MinBytes:               1,    // 1B
		MaxBytes:               10e6, // 10MB
		ReadLagInterval:        30 * time.Second,
		HeartbeatInterval:      3 * time.Second,
		CommitInterval:         0,
		PartitionWatchInterval: 5 * time.Second,
		WatchPartitionChanges:  true,
		SessionTimeout:         workerConfig.MaxProcessTime + (5 * time.Second),
		RebalanceTimeout:       workerConfig.MaxProcessTime + (5 * time.Second),
		RetentionTime:          365 * 24 * time.Hour,
		StartOffset:            kafka.FirstOffset,
		ReadBackoffMin:         100 * time.Millisecond,
		ReadBackoffMax:         1 * time.Second,
		MaxAttempts:            5,
		GroupBalancers:         []kafka.GroupBalancer{kafka.RangeGroupBalancer{}, kafka.RoundRobinGroupBalancer{}}, // kafka-go did not support sticky group balancer :(
	}

	if workerConfig.UseTLS {
		config.Dialer = &kafka.Dialer{
			TLS: &tls.Config{},
		}
		config.Dialer.TLS.InsecureSkipVerify = workerConfig.TLSSkipVerify
	}

	return &KafkaWorker{
		consumer: kafka.NewReader(config),
		running:  false,
		error:    nil,
		config:   workerConfig,
		handlers: handlers,
	}
}
