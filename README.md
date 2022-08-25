
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
Idempotent Worker: Required
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
Idempotent Worker: Required
UseCase: Generic kafka worker, limited by partition number

`go test -v -run Test_Example_3 ./example`

### 04 Parallel worker
Received message in batch, process all message at the same time, once all message processed, commit all messages.
Order: NO
Worker Process Failed: handle it manually (logging ?)
Speed: Best
Idempotent Worker: Required
UseCase: Process that want to complete as fast as possible, and don't need to be ORDERED, e.g. Logging, Broadcasting

`go test -v -run Test_Example_4 ./example`

### 05 Key concurrent
Received message in batch, fork each `KEY` into separate thread, Each `KEY` will process message in serial (ordered), and commit once per partition, Use case similar to Example 1, but have better process speed, due to concurrency
Order: yes (Message.Key)
Worker Process Failed: handle it manually (logging ?)
Speed: Good+
Idempotent Worker: Required
UseCase: Generic kafka worker, limited by partition number

`go test -v -run Test_Example_5 ./example`

### 06 Worker's Logging, Tracing, Metric
Worker level tracing
logging (https://github.com/uber-go/zap)
tracing (https://github.com/open-telemetry/opentelemetry-go)
metric (https://github.com/prometheus/client_golang)

`go test -v -run Test_Example_6 ./example`

Trace: http://localhost:16686/search?&limit=20&service=demo-kafka-worker&operation=kafka_consumer_worker_example_6_worker

### 07 Batch + Worker Logging, Tracing, Metric
Batch + Worker level tracing
logging (https://github.com/uber-go/zap)
tracing (https://github.com/open-telemetry/opentelemetry-go)
metric (https://github.com/prometheus/client_golang)

`go test -v -run Test_Example_7 ./example`

Trace: http://localhost:16686/search?&limit=20&service=demo-kafka-worker&operation=kafka_consumer_worker_example_7

### 08 Concurrent limiter
Fork all message into each thread, but limit maximum concurrent to `2` workers
Order: NO
Worker Process Failed: handle it manually (logging ?)
Speed: Good++
Idempotent Worker: Required
UseCase: Parallel worker that have resource constraint

`go test -v -run Test_Example_8 ./example`

Trace: http://localhost:16686/search?&limit=20&service=demo-kafka-worker&operation=kafka_consumer_worker_example_8

### 09 Partition concurrent limit
Only allow `1` partition process at a time and limit workers to `3` (by key)
Order: yes (Message.Key)
Worker Process Failed: handle it manually (logging ?)
Speed: Good+
Idempotent Worker: Required
UseCase: Parallel Key worker that have resource constraint

`go test -v -run Test_Example_9 ./example`

Trace: http://localhost:16686/search?&limit=20&service=demo-kafka-worker&operation=kafka_consumer_worker_example_9

### 10 Exactly once
Only allow `1` partition process at a time, then commit 1 message at a time
Order: yes (Message.Key)
Worker Process Failed: handle it manually (logging ?)
Speed: POOR-
Idempotent Worker: No need (worker use database transaction)
UseCase: Job that need to process once and cannot be `reverse` or `compensate`

`go test -v -run Test_Example_10 ./example`


### 11 Stop partition on worker failed
Process message in partition, if any of message in partition failed to process, worker will freeze that partition, and stop process the partition
Order: yes (Message.Key)
Worker Process Failed: The partition that cause the error will not process anymore, resume by update worker code to handle that error case, or skip that error message
Speed: Good+ | will halt completely if anything error in that partition
Idempotent Worker: Required
UseCase: Job that order are required and cannot process if any messages are missing (Stateful), e.g. Bank transfer transaction summary

`go test -v -run Test_Example_11 ./example`

Trace: http://localhost:16686/search?&limit=20&service=demo-kafka-worker&operation=kafka_consumer_worker_example_11