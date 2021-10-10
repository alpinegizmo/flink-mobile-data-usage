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

## The other jobs

While `KafkaProducerJob` is running, you can run any of the other jobs.

