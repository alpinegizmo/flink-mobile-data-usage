# setting up fling

curl -O https://dlcdn.apache.org/flink/flink-1.14.0/flink-1.14.0-bin-scala_2.12.tgz
tar -xzf flink-*.tgz
cd flink-1.14.0
ls -la bin/
./bin/start-cluster.sh
open http://localhost:8081

./bin/flink run examples/streaming/WordCount.jar
tail log/flink-*-taskexecutor-*.out


# setting up dev environment

mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-java \
    -DarchetypeVersion=1.14.0 \
    -DgroupId=frauddetection \
    -DartifactId=frauddetection \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false