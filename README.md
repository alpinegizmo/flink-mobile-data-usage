# Flink Mobile Data Usage

## Getting Started

Import this repo into an IDE, such as IntelliJ, as a Gradle project.

## KafkaProducerJob

```
docker-compose up -d
```

will start up Kafka.

Running `KafkaProducerJob` will populate an _input_ topic with UsageRecords.
You can run this job in your IDE.

## TotalUsageBatchJob and TotalUsageStreamingJob

Two flavors of the same application, one for BATCH and one for STREAMING.
While these are running the Flink Web UI is available at http://localhost:8081.
