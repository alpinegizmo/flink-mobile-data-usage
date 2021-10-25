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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;

public class UsageRecordDeserializationSchema implements DeserializationSchema<UsageRecord> {
    private static final long serialVersionUID = 1L;

    private static final ObjectMapper objectMapper =
            JsonMapper.builder().build().registerModule(new JavaTimeModule());

    @Override
    public UsageRecord deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, UsageRecord.class);
    }

    @Override
    public boolean isEndOfStream(UsageRecord nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UsageRecord> getProducedType() {
        return TypeInformation.of(UsageRecord.class);
    }
}
