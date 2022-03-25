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
import org.apache.flink.types.Row;

import com.ververica.flink.example.datausage.records.UsageRecord;

import java.time.Instant;
import java.util.Random;

public class UsageRecordGenerator extends RichParallelSourceFunction<Row> {

    private static final long serialVersionUID = 1L;

    public static final int NUMBER_OF_ACCOUNTS_PER_INSTANCE = 10;

    private volatile boolean running = true;
    private int indexOfThisSubtask;

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {

        this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        UsageRecordIterator usageRecordIterator = new UsageRecordIterator(indexOfThisSubtask);

        while (running) {
            UsageRecord event = usageRecordIterator.next();
            ctx.collect(Row.of(
                    event.account, event.bytesUsed, event.ts
            ));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    static class UsageRecordIterator {

        private Random random;
        private int indexOfThisSubtask;

        public UsageRecordIterator(int indexOfThisSubtask) {
            random = new Random();
            this.indexOfThisSubtask = indexOfThisSubtask;
        }

        public UsageRecord next() {
            Instant ts = Instant.now();
            String account = nextAccountForInstance();

            int bytesUsed = 18 * random.nextInt(10_000_000);
            return new UsageRecord(ts, account, bytesUsed);
        }

        private String nextAccountForInstance() {
            return UsageRecord.accountForSubtaskAndIndex(
                    indexOfThisSubtask, random.nextInt(NUMBER_OF_ACCOUNTS_PER_INSTANCE));
        }
    }

    public static TypeInformation<Row> typeProduced() {
        return Types.ROW_NAMED(
                new String[] {"account", "bytesUsed", "ts"},
                Types.STRING,
                Types.LONG,
                TypeInformation.of(Instant.class));
    }
}
