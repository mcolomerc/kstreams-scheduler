package org.example.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.CustomRecord;

import java.util.Map;

public class CustomRecordDeserializer implements Deserializer<CustomRecord> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public CustomRecord deserialize(String topic, byte[] data) {
        try {
            if (data == null || data.length == 0) {
                return null; // Return null for empty data
            }
            return objectMapper.readValue(data, CustomRecord.class);
        } catch (Exception e) {
            throw new RuntimeException("Error when deserializing byte[] to CustomRecord");
        }
    }

    @Override
    public void close() {
    }
}

