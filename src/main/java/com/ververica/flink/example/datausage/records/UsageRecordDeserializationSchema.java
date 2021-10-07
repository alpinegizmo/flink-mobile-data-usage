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
