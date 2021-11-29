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

package com.ververica.flink.example.datausage.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import com.ververica.flink.example.datausage.records.UsageRecord;

import java.time.Instant;

public class AccountUpdateGenerator extends RichParallelSourceFunction<Row> {

    private static final long serialVersionUID = 1L;

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();

        for (int i = 0; i < UsageRecordGenerator.NUMBER_OF_ACCOUNTS_PER_INSTANCE; i++) {
            ctx.collect(
                    Row.of(
                            UsageRecord.accountForSubtaskAndIndex(subtask, i),
                            10_000_000_000L,
                            Instant.parse("2021-01-01T00:00:00.000Z")));
        }

        ctx.emitWatermark(Watermark.MAX_WATERMARK);

        while (running) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static TypeInformation<Row> typeProduced() {
        return Types.ROW_NAMED(
                new String[] {"id", "quota", "ts"},
                Types.STRING,
                Types.LONG,
                TypeInformation.of(Instant.class));
    }
}
