
# kafka-go-worker

A kafka' pipeline framework build on to of [kafka-go](https://github.com/segmentio/kafka-go), aim to help build kafka batch processors without write lower level code to handle message/events

## Example

before start example
- Start required software (kafka, jaeger)
    - `docker-compose -f ./example/docker-compose.yml up -d`
        - kafka `localhost:9090`
        - jaeger web http://localhost:16686
- Run seed kafka topic and message
    - `go test -v -run Test_Seed_Kafka ./example`

### 01 Simple worker
Received message in batch, process message 1 by 1 until all messages processed then commit ALL message in batch if some messages failed it still got COMMITTED Use case generic kafka pipeline, need handle failed message later without blocking the stream

`go test -v -run Test_Example_1 ./example`

### 02 Graceful shutdown, and Readiness probe
Similar to Example1 But included graceful shutdown, and readiness probe

`go test -v -run Test_Example_2 ./example`
