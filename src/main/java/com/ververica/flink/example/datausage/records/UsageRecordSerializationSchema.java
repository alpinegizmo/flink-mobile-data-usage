package com.ververica.flink.example.datausage.records;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerRecord;

public class UsageRecordSerializationSchema implements KafkaRecordSerializationSchema<UsageRecord> {

    private String topic;
    private static final ObjectMapper objectMapper =
            JsonMapper.builder()
                    .build()
                    .registerModule(new JavaTimeModule())
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public UsageRecordSerializationSchema() {}

    public UsageRecordSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            UsageRecord element, KafkaSinkContext context, Long timestamp) {

        try {
            return new ProducerRecord<>(
                    topic,
                    null,
                    element.ts.toEpochMilli(),
                    null,
                    objectMapper.writeValueAsBytes(element));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + element, e);
        }
    }
}
