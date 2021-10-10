package com.ververica.flink.example.datausage;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.flink.example.datausage.records.UsageRecord;
import com.ververica.flink.example.datausage.records.UsageRecordSerializationSchema;
import com.ververica.flink.example.datausage.sources.UsageRecordGenerator;

import java.util.Properties;

public class KafkaProducerJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000L);
        env.setParallelism(2);

        final ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.get("topic", "input");
        String brokers = params.get("bootstrap.servers", "localhost:9092");

        Properties kafkaProps = new Properties();
        // in production, use a much longer timeout than this
        kafkaProps.put("transaction.timeout.ms", 600000);

        KafkaSink<UsageRecord> sink =
                KafkaSink.<UsageRecord>builder()
                        .setBootstrapServers(brokers)
                        .setKafkaProducerConfig(kafkaProps)
                        .setRecordSerializer(new UsageRecordSerializationSchema(topic))
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("usage-record-producer")
                        .build();

        env.addSource(new UsageRecordGenerator()).sinkTo(sink);

        env.execute();
    }
}
