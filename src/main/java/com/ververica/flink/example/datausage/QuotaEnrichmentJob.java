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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.ververica.flink.example.datausage.sources.AccountUpdateGenerator;

public class QuotaEnrichmentJob {
    public static void main(String[] args) throws Exception {

        /******************************************************************************************
         * Setting up environment
         ******************************************************************************************/
        final Configuration flinkConfig = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        env.setParallelism(4);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        /******************************************************************************************
         * Create table using the kafka connector
         ******************************************************************************************/

        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE TABLE usage (",
                        "  account STRING,",
                        "  bytesUsed BIGINT,",
                        "  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',",
                        "  WATERMARK FOR ts AS ts",
                        ") WITH (",
                        "  'connector' = 'kafka',",
                        "  'topic' = 'input',",
                        "  'properties.bootstrap.servers' = 'localhost:9092',",
                        "  'scan.startup.mode' = 'earliest-offset',",
                        "  'format' = 'json'",
                        ")"));


        /******************************************************************************************
         * Creating a data stream from the account update generator
         ******************************************************************************************/

        DataStream<Row> accountUpdateStream =
                env.addSource(new AccountUpdateGenerator())
                        .returns(AccountUpdateGenerator.typeProduced());


        /******************************************************************************************
         * Setting up a table for account updates
         ******************************************************************************************/

        Schema accountUpdateSchema =
                Schema.newBuilder()
                        .column("id", "STRING NOT NULL")
                        .column("quota", "BIGINT")
                        .column("ts", "TIMESTAMP_LTZ(3)")
                        .watermark("ts", "SOURCE_WATERMARK()")
                        .primaryKey("id")
                        .build();
        Table accountUpdates = tEnv.fromChangelogStream(accountUpdateStream, accountUpdateSchema);
        tEnv.createTemporaryView("account", accountUpdates);

        /******************************************************************************************
         * Joining the usage table with the account table
         ******************************************************************************************/

        Table enrichedRecords =
                tEnv.sqlQuery(
                        String.join(
                                "\n",
                                "SELECT usage.account, usage.bytesUsed, account.quota, usage.ts",
                                "FROM usage JOIN account FOR SYSTEM_TIME AS OF usage.ts",
                                "ON usage.account = account.id",
                                "ORDER BY usage.ts"));


        /******************************************************************************************
         * Printing the records
         ******************************************************************************************/

        enrichedRecords.execute().print();
    }
}
