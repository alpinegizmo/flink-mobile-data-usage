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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.ververica.flink.example.datausage.records.UsageRecord;
import com.ververica.flink.example.datausage.records.UsageRecordDeserializationSchema;

import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;

public class TotalUsageBatchJob {

    public static void main(String[] args) {

        /******************************************************************************************
         * Getting the topic and the servers
         ******************************************************************************************/

        final ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.get("topic", "input");
        String brokers = params.get("bootstrap.servers", "localhost:9092");


        /******************************************************************************************
         * Setting up environment
         ******************************************************************************************/

        final Configuration flinkConfig = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        /******************************************************************************************
         * Creating the bounded Kafka source
         ******************************************************************************************/

        KafkaSource<UsageRecord> source =
                KafkaSource.<UsageRecord>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(topic)
                        .setStartingOffsets(
                                OffsetsInitializer.timestamp(
                                        Instant.parse("2021-10-01T00:00:00.000Z").toEpochMilli()))
                        .setBounded(
                                OffsetsInitializer.timestamp(
                                        Instant.parse("2021-10-31T23:59:59.999Z").toEpochMilli()))
                        .setValueOnlyDeserializer(new UsageRecordDeserializationSchema())
                        .build();


        /******************************************************************************************
         * Creating the data stream
         ******************************************************************************************/

        DataStream<UsageRecord> records =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


        /******************************************************************************************
         * Creating a result table
         ******************************************************************************************/

        Table results =
                tEnv.fromDataStream(records)
                        .groupBy($("account"))
                        .select(
                                $("account"),
                                $("bytesUsed").sum().as("totalUsage"),
                                $("ts").max().as("asOf"));


        /******************************************************************************************
         * Executing and printing the results
         ******************************************************************************************/

        results.execute().print();
    }
}
