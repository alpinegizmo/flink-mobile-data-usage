package com.ververica.flink.example.datausage;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.ververica.flink.example.datausage.records.EnrichedUsageRecord;
import com.ververica.flink.example.datausage.sources.AccountUpdateGenerator;

public class UsageAlertingProcessFunctionJob {
    public static void main(String[] args) throws Exception {

        final Configuration flinkConfig = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        env.setParallelism(2);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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

        DataStream<Row> accountUpdateStream =
                env.addSource(new AccountUpdateGenerator())
                        .returns(AccountUpdateGenerator.typeProduced());

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

        Table enrichedRecords =
                tEnv.sqlQuery(
                        String.join(
                                "\n",
                                "SELECT",
                                "  usage.account AS account,",
                                "  usage.bytesUsed AS bytesUsed,",
                                "  account.quota AS quota,",
                                "  usage.ts AS ts,",
                                "  EXTRACT(YEAR from usage.ts) AS billingYear,",
                                "  EXTRACT(MONTH from usage.ts) AS billingMonth",
                                "FROM usage JOIN account FOR SYSTEM_TIME AS OF usage.ts",
                                "ON usage.account = account.id",
                                "ORDER BY usage.ts"));

        DataStream<EnrichedUsageRecord> enrichedStream =
                tEnv.toDataStream(enrichedRecords, EnrichedUsageRecord.class);

        enrichedStream
                .keyBy(e -> e.keyByAccountYearMonthQuota())
                .process(new UsageAlertingFunction())
                .print();

        env.execute();
    }

    private static class UsageAlertingFunction
            extends KeyedProcessFunction<String, EnrichedUsageRecord, String> {

        ReducingState<Long> rollingUsage;
        ValueState<Boolean> alerted;

        @Override
        public void open(Configuration parameters) throws Exception {

            final ReducingStateDescriptor<Long> rollingUsageStateDesc =
                    new ReducingStateDescriptor<>("rolling-sum", new Sum(), Types.LONG());
            rollingUsage = getRuntimeContext().getReducingState(rollingUsageStateDesc);

            final ValueStateDescriptor<Boolean> alertedStateDescriptor =
                    new ValueStateDescriptor<Boolean>("alerted", Types.BOOLEAN());
            alerted = getRuntimeContext().getState(alertedStateDescriptor);

            // TODO: arrange for state to be cleared after each month
        }

        @Override
        public void processElement(EnrichedUsageRecord record, Context ctx, Collector<String> out)
                throws Exception {

            rollingUsage.add(record.bytesUsed);
            long total = rollingUsage.get();

            if (alerted.value() == null && total > (0.9 * record.quota)) {
                out.collect(
                        String.format(
                                "WARNING: as of %s account %s has used %d out of %d bytes",
                                record.ts, record.account, total, record.quota));

                alerted.update(true);
            }
        }
    }

    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }
}
