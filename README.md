# Flink Mobile Data Usage

## Getting Started

Import this repo into an IDE, such as IntelliJ, as a Gradle project.

### KafkaProducerJob

Before running any of these application, first use docker-compose to start
up a Kafka cluster:

```
docker-compose up -d
```

Running `KafkaProducerJob` in your IDE will populate an _input_ topic with UsageRecords.
After having run this you can run any of the other applications in this project
(all of which expect to read from Kafka).

### UsageAlertingProcessFunctionJob

While this job is running in the IDE you can use Flink's web UI at http://localhost:8081.

### Setting up a Flink cluster

```bash
curl -O https://dlcdn.apache.org/flink/flink-1.14.0/flink-1.14.0-bin-scala_2.12.tgz
tar -xzf flink-*.tgz
cd flink-1.14.0
ls -la bin/
./bin/start-cluster.sh
```

Then browse to http://localhost:8081.

To see the cluster run the streaming WordCount example:

```bash
./bin/flink run examples/streaming/WordCount.jar
tail log/flink-*-taskexecutor-*.out
```

### Running this project in a cluster

If you have already started a cluster, stop it and reconfigure Flink to have more resources
by modifing `flink-1.14.0/conf/flink-conf.yaml` so that `taskmanager.numberOfTaskSlots`
is set to 8.

Then with `flink-1.14.0/bin` in your PATH:

```bash
./gradlew build
start-cluster.sh
flink run -d build/libs/flink-mobile-data-usage-0.2-all.jar
flink run -d -c com.ververica.flink.example.datausage.UsageAlertingProcessFunctionJob \
  build/libs/flink-mobile-data-usage-0.2-all.jar --webui false
```

## How to create a new maven project for your own Flink application

```bash
curl https://flink.apache.org/q/quickstart.sh | bash -s 1.14.0
```

See [Project Configuration](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/project-configuration) in the Flink docs for more information.
