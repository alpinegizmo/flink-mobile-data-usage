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

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.ververica.flink.example.datausage.records.UsageRecord;

import java.time.Instant;
import java.util.Random;

public class UsageRecordGenerator extends RichParallelSourceFunction<UsageRecord> {

    public static final int NUMBER_OF_ACCOUNTS_PER_INSTANCE = 10000;
    public static final int EVENTS_PER_DAY_PER_ACCOUNT = 4;
    public static final int MILLISECONDS_PER_DAY = 86_400_000;
    public static final long DELTA_T =
            MILLISECONDS_PER_DAY / (NUMBER_OF_ACCOUNTS_PER_INSTANCE * EVENTS_PER_DAY_PER_ACCOUNT);
    public static final Instant BEGINNING = Instant.parse("2021-10-01T00:00:00.00Z");
    public static final Instant SLOWDOWN = Instant.parse("2021-10-26T17:00:00.00Z");

    private volatile boolean running = true;
    private int indexOfThisSubtask;

    @Override
    public void run(SourceContext<UsageRecord> ctx) throws Exception {

        this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        UsageRecordIterator usageRecordIterator = new UsageRecordIterator(indexOfThisSubtask);
        long startTime = System.currentTimeMillis();

        while (running) {
            UsageRecord event = usageRecordIterator.next();
            ctx.collect(event);
            // slow down once the timestamps catch up to the date of Flink Forward
            if (event.ts.compareTo(SLOWDOWN) > 0) {
                Thread.sleep(DELTA_T);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    static class UsageRecordIterator {

        private long nextTimestamp;
        private Random random;
        private int indexOfThisSubtask;

        public UsageRecordIterator(int indexOfThisSubtask) {
            nextTimestamp = BEGINNING.toEpochMilli();
            random = new Random();
            this.indexOfThisSubtask = indexOfThisSubtask;
        }

        public UsageRecord next() {
            Instant ts = Instant.ofEpochMilli(nextTimestamp);
            nextTimestamp += DELTA_T;
            String account = nextAccountForInstance();

            int bytesUsed = 18 * random.nextInt(10_000_000);
            return new UsageRecord(ts, account, bytesUsed);
        }

        private String nextAccountForInstance() {
            return UsageRecord.accountForSubtaskAndIndex(
                    indexOfThisSubtask, random.nextInt(NUMBER_OF_ACCOUNTS_PER_INSTANCE));
        }
    }
}
