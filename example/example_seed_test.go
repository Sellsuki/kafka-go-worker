package example

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"strconv"
	"testing"
	"time"
)

func Test_Seed_Kafka(t *testing.T) {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		t.Error(err)
		return
	}

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             workerConfig.TopicName,
		NumPartitions:     3,
		ReplicationFactor: -1,
	})
	if err != nil {
		t.Error(err)
		return
	}

	w := kafka.Writer{
		Addr:                   kafka.TCP(workerConfig.KafkaBrokers...),
		Topic:                  workerConfig.TopicName,
		BatchSize:              100,
		BatchTimeout:           300 * time.Millisecond,
		Balancer:               kafka.Murmur2Balancer{Consistent: true},
		AllowAutoTopicCreation: true,
	}

	msgCount := 100
	msgs := make([]kafka.Message, msgCount)

	for i := 0; i < msgCount; i++ {
		msgs[i] = kafka.Message{
			Key:   []byte(strconv.Itoa(randRange(1, 10))),
			Value: nil,
		}
	}

	errorIdx := randRange(50, 70)
	msgs[errorIdx].Value = []byte("error")
	log.Printf("error at %d", errorIdx)

	err = w.WriteMessages(context.Background(), msgs...)
	if err != nil {
		t.Error(err)
		return
	}

	log.Printf("Seeded %d messages to %s", msgCount, workerConfig.TopicName)
}
