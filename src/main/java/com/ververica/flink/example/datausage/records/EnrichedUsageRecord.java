package com.ververica.flink.example.datausage.records;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;

public class EnrichedUsageRecord {

    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            timezone = "UTC")
    public Instant ts;

    public String account;
    public long bytesUsed;
    public long quota;
    public long billingYear;
    public long billingMonth;

    public EnrichedUsageRecord() {}

    @Override
    public String toString() {
        return "EnrichedUsageRecord{"
                + "ts="
                + ts
                + ", account='"
                + account
                + '\''
                + ", bytesUsed="
                + bytesUsed
                + ", quota="
                + quota
                + ", billingYear="
                + billingYear
                + ", billingMonth="
                + billingMonth
                + '}';
    }

    public String keyByAccountYearMonthQuota() {
        return String.format(
                "%s/%d/%d/%d", this.account, this.billingYear, this.billingMonth, this.quota);
    }
}
