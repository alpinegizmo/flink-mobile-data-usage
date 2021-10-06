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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.ververica.flink.example.datausage.records.UsageRecord;
import com.ververica.flink.example.datausage.records.UsageRecordDeserializationSchema;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

public class TotalUsageStreamingJob {

	public static final Time WINDOW_SIZE = Time.of(30, TimeUnit.DAYS);

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		String topic = params.get("topic", "input");
		String brokers = params.get("bootstrap.servers", "localhost:9092");

		final Configuration flinkConfig = new Configuration();
		final StreamExecutionEnvironment env =
				StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
		env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		KafkaSource<UsageRecord> source = KafkaSource.<UsageRecord>builder()
				.setBootstrapServers(brokers)
				.setTopics(topic)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new UsageRecordDeserializationSchema())
				.build();

		DataStream<UsageRecord> records = env
				.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

		Table results = tEnv.fromDataStream(records)
				.groupBy($("account"))
				.select($("account"),
						$("bytesUsed").sum().as("totalUsage"),
						$("ts").max().as("asOf"));

		results.execute().print();
	}
}
