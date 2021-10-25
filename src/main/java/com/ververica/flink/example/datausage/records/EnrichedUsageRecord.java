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
