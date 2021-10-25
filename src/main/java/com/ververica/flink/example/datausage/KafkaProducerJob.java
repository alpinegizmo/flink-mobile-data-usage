/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        env.setParallelism(4);

        final ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.get("topic", "input");
        String brokers = params.get("bootstrap.servers", "localhost:9092");

        Properties kafkaProps = new Properties();
        kafkaProps.put("transaction.timeout.ms", 600000); // in production, use a longer timeout

        KafkaSink<UsageRecord> sink =
                KafkaSink.<UsageRecord>builder()
                        .setBootstrapServers(brokers)
                        .setKafkaProducerConfig(kafkaProps)
                        .setRecordSerializer(new UsageRecordSerializationSchema(topic))
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("usage-record-producer")
                        .build();

        env.addSource(new UsageRecordGenerator()).sinkTo(sink);

        env.execute("KafkaProducerJob");
    }
}
