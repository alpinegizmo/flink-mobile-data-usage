package com.ververica.flink.example.datausage.sources;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.ververica.flink.example.datausage.records.UsageRecord;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class UsageRecordGenerator implements ParallelSourceFunction<UsageRecord> {

    private static final List<String> accounts = Arrays.asList("0612345678", "0176554423");

    public static final Time WINDOW_SIZE = Time.of(30, TimeUnit.DAYS);
    public static final int NUMBER_OF_ACCOUNTS = accounts.size();
    public static final int EVENTS_PER_WINDOW = 1000;
    public static final int SPEEDUP_FACTOR = 100_000;
    public static final long DELTA_T =
            WINDOW_SIZE.toMilliseconds() / NUMBER_OF_ACCOUNTS / EVENTS_PER_WINDOW;
    public static final Instant BEGINNING = Instant.parse("2021-10-01T00:00:00.00Z");

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<UsageRecord> ctx) throws Exception {

        UsageRecordIterator usageRecordIterator = new UsageRecordIterator();

        while (running) {
            UsageRecord event = usageRecordIterator.next();
            ctx.collect(event);
            Thread.sleep(DELTA_T / SPEEDUP_FACTOR);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    static class UsageRecordIterator {

        private long nextTimestamp;
        private int nextIndex;
        private Random random;

        public UsageRecordIterator() {
            nextTimestamp = BEGINNING.toEpochMilli();
            nextIndex = 0;
            random = new Random();
        }

        UsageRecord next() {
            Instant ts = Instant.ofEpochMilli(nextTimestamp);
            nextTimestamp += DELTA_T;
            String account = accounts.get(random.nextInt(NUMBER_OF_ACCOUNTS));
            int bytesUsed = 100 * random.nextInt(50_000);
            return new UsageRecord(ts, account, bytesUsed);
        }
    }
}
