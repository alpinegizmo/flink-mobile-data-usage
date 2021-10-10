package com.ververica.flink.example.datausage.records;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;
import java.util.Objects;

import static com.ververica.flink.example.datausage.sources.UsageRecordGenerator.NUMBER_OF_ACCOUNTS_PER_INSTANCE;

/**
 * This class is set up as a Flink POJO so that it can be serialized by Flink's PojoSerializer.
 * That's why there is an empty, default constructor, and the fields are public (though we could
 * have implemented getters and setters instead).
 */
public class UsageRecord {

    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            timezone = "UTC")
    public Instant ts;

    public String account;
    public long bytesUsed;

    public UsageRecord() {}

    public UsageRecord(final Instant ts, final String account, final int bytesUsed) {
        this.ts = ts;
        this.account = account;
        this.bytesUsed = bytesUsed;
    }

    @Override
    public String toString() {
        return "Usage{"
                + "ts="
                + ts
                + ", account='"
                + account
                + '\''
                + ", bytesUsed='"
                + bytesUsed
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UsageRecord that = (UsageRecord) o;
        return account.equals(that.account) && bytesUsed == that.bytesUsed && ts.equals(that.ts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ts, account, bytesUsed);
    }

    public static String accountForSubtaskAndIndex(int subtask, int index) {
        return String.format("%06d", (NUMBER_OF_ACCOUNTS_PER_INSTANCE * subtask) + index);
    }
}
