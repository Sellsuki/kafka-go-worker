
# kafka-go-worker

A kafka' pipeline framework build on to of [kafka-go](https://github.com/segmentio/kafka-go), aim to help build kafka batch processors, including `at-most once`, `at-least once`, `~exactly once`, `graceful shutdown`, `readiness probe`, `logger`, `jaeger tracer`, `prometheus metric`, `concurrency`, `partition concurrent`, `key concurrent`, `freeze partition on failed`.

## Example

### Before start example first time
- Start required software (kafka, jaeger)
    - `docker-compose -f ./example/docker-compose.yml up -d`
        - kafka `localhost:9090`
        - jaeger web http://localhost:16686
- Run seed kafka topic and message
    - `go test -v -run Test_Seed_Kafka ./example`

### Before each example
- Stop current example
- Delete the Consumer group (default: `worker_name`)
  - Or re-seed data into topic again


### 01 Simple worker
Received message in batch, process message 1 by 1 until all messages processed then commit ALL message in batch if some messages failed it still got COMMITTED Use case generic kafka pipeline, need handle failed message later without blocking the stream
Order: yes
Worker Process Failed: handle it manually (logging ?)
Speed: Poor
UseCase: Generic kafka worker and single core ?

`go test -v -run Test_Example_1 ./example`

### 02 Graceful shutdown, and Readiness probe
Similar to Example1 But included graceful shutdown, and readiness probe

`go test -v -run Test_Example_2 ./example`

### 03 Partition concurrent
Received message in batch, fork each partition into separate thread, Each partition will process message in serial (ordered), and commit once per partition,Use case similar to Example 1, but have better process speed, due to concurrency
Order: yes (Partition)
Worker Process Failed: handle it manually (logging ?)
Speed: Good
UseCase: Generic kafka worker, limited by partition number

`go test -v -run Test_Example_3 ./example`

### 04 Parallel worker
Received message in batch, process all message at the same time, once all message processed, commit all messages.
Order: NO
Worker Process Failed: handle it manually (logging ?)
Speed: Best
UseCase: Process that want to complete as fast as possible, and don't need to be ORDERED, e.g. Logging, Broadcasting

`go test -v -run Test_Example_4 ./example`

### 05 Partition concurrent
Received message in batch, fork each `KEY` into separate thread, Each `KEY` will process message in serial (ordered), and commit once per partition, Use case similar to Example 1, but have better process speed, due to concurrency
Order: yes (Message.Key)
Worker Process Failed: handle it manually (logging ?)
Speed: Good++
UseCase: Generic kafka worker, limited by partition number

`go test -v -run Test_Example_5 ./example`

### 06 Worker's Logging, Tracing, Metric
Worker level tracing
logging (https://github.com/uber-go/zap)
tracing (https://github.com/open-telemetry/opentelemetry-go)
metric (https://github.com/prometheus/client_golang)

`go test -v -run Test_Example_6 ./example`

Trace: http://localhost:16686/search?&limit=20&service=demo-kafka-worker

### 07 Batch + Worker Logging, Tracing, Metric
Batch + Worker level tracing
logging (https://github.com/uber-go/zap)
tracing (https://github.com/open-telemetry/opentelemetry-go)
metric (https://github.com/prometheus/client_golang)

`go test -v -run Test_Example_7 ./example`

Trace: http://localhost:16686/search?&limit=20&service=demo-kafka-worker
v