package com.ververica.flink.example.datausage.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import java.time.Instant;

import static com.ververica.flink.example.datausage.sources.UsageRecordGenerator.accounts;

public class AccountUpdateGenerator implements SourceFunction<Row> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        ctx.collect(
                Row.of(accounts.get(0), 5_000_000_000L, Instant.parse("2021-01-01T00:00:00.000Z")));

        ctx.collect(
                Row.of(accounts.get(1), 5_000_000_000L, Instant.parse("2021-01-01T00:00:00.000Z")));

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
