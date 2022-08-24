package handler

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
)

// withOtelTracer Recommended to use this before the worker, due to span context will get override by next message in slice
func withOtelTracer(packageName, spanName, workerName string, resumeTrace ...bool) Handler {
	tracer := otel.Tracer(packageName)

	return func(c *Context) error {
		if len(c.Messages) == 0 {
			return c.Next()
		}

		if len(resumeTrace) > 0 && resumeTrace[0] == true {
			propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
			carrier := propagation.MapCarrier{}

			for _, h := range c.Messages[len(c.Messages)-1].Headers {
				carrier.Set(h.Key, string(h.Value))
			}

			c.ctx = propagator.Extract(c.Context(), carrier)
		}

		kafkaConfig := c.Consumer.Config()
		kafkaStat := c.Consumer.Stats()

		spanOptions := []trace.SpanStartOption{
			trace.WithAttributes(semconv.MessagingKafkaClientIDKey.String(kafkaStat.ClientID)),
			trace.WithAttributes(semconv.MessagingKafkaPartitionKey.String(kafkaStat.Partition)),
			trace.WithAttributes(semconv.MessagingKafkaConsumerGroupKey.String(kafkaConfig.GroupID)),
			//trace.WithAttributes(semconv.MessagingKafkaMessageKeyKey.String()),
			trace.WithAttributes(semconv.MessagingDestinationKey.String(kafkaStat.Topic)),
			trace.WithAttributes(attribute.String("worker_name", workerName)),
			trace.WithSpanKind(trace.SpanKindServer),
		}

		ctx, span := tracer.Start(c.Context(), spanName, spanOptions...)
		defer span.End()

		c.ctx = ctx

		return c.Next()
	}
}
