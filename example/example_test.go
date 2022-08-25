//go:build !example

package example

import (
	"context"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	kafka_consumer_worker "kafka-go-worker"
	"log"
	"math/rand"
	"time"
)

const jaegerCollectorEndpoint = "http://localhost:14268/api/traces"

var workerConfig = kafka_consumer_worker.WorkerConfig{
	TopicName:       "topic_name",
	WorkerName:      "worker_name",
	KafkaBrokers:    []string{"127.0.0.1:9092"},
	BatchSize:       10, // The batch number
	MaxWait:         1 * time.Second,
	BackoffDelay:    time.Second,
	MaxBackoffDelay: time.Minute,
	MaxProcessTime:  time.Minute,
}
var prom = prometheus.NewRegistry()
var tracer = otel.Tracer("namespace_here")

func initLogger() {
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	config.EncoderConfig.MessageKey = "message"
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	config.Encoding = "console"

	lg, err := config.Build()
	if err != nil {
		log.Fatalf("Error build logger: %s\n", err)
	}
	defer lg.Sync()

	zap.ReplaceGlobals(lg)
}

func initTracer() {
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(jaegerCollectorEndpoint)))
	if err != nil {
		zap.L().Fatal("Error init Jaeger exporter", zap.Error(err))
	}

	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("demo-kafka-worker"),
			semconv.ServiceVersionKey.String("v0.0.0"),
			attribute.String("environment", "demo"),
		),
	)
	if err != nil {
		zap.L().Fatal("Error init Jaeger resource", zap.Error(err))
	}

	tp := trace.NewTracerProvider(
		// Always be sure to batch in production.
		trace.WithBatcher(exp),
		// Record information about this application in a Resource.
		trace.WithResource(r),
	)

	otel.SetTracerProvider(tp)
	zap.L().Info("Tracer initialized")
}

func loopPrintMetric() {
	for {
		metrics, err := prom.Gather()
		if err != nil {
			zap.L().Warn("failed to gather metrics", zap.Error(err))
		}
		for _, m := range metrics {
			for _, mx := range m.Metric {
				zap.L().Info("metrics", zap.String("name", m.GetName()), zap.String("value", mx.Counter.String()), zap.String("summary", mx.Summary.String()), zap.Any("labels", mx.Label))
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func demoWorker(ctx context.Context, msg kafka.Message) error {
	zap.L().Info("Received message", zap.String("topic", msg.Topic), zap.Int("partition", msg.Partition), zap.Int64("offset", msg.Offset), zap.ByteString("key", msg.Key), zap.ByteString("payload", msg.Value))

	ctx, span := tracer.Start(ctx, "demoWorker")
	defer span.End()
	span.SetAttributes(semconv.MessagingDestinationKey.String(msg.Topic))
	span.SetAttributes(semconv.MessagingKafkaPartitionKey.Int(msg.Partition))
	span.SetAttributes(attribute.Int64("offset", msg.Offset))
	span.SetAttributes(semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key)))

	time.Sleep(100 * time.Millisecond)

	if string(msg.Value) == "error" {
		span.SetStatus(codes.Error, "worker some what error")

		zap.L().Warn("Worker failed", zap.String("topic", msg.Topic), zap.Int("partition", msg.Partition), zap.Int64("offset", msg.Offset), zap.ByteString("key", msg.Key))
		return errors.New("I don't like this message")
	}

	zap.L().Info("Worker Success", zap.String("topic", msg.Topic), zap.Int("partition", msg.Partition), zap.Int64("offset", msg.Offset), zap.ByteString("key", msg.Key))
	return nil
}

func randRange(min, max int) int {
	return rand.Intn(max-min) + min
}
