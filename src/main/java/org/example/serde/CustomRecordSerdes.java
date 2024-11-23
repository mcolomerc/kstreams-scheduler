package org.example.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.example.CustomRecord;

public class CustomRecordSerdes implements Serde<CustomRecord> {
    private final Serializer<CustomRecord> serializer = new CustomRecordSerializer();
    private final Deserializer<CustomRecord> deserializer = new CustomRecordDeserializer();

    @Override
    public Serializer<CustomRecord> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<CustomRecord> deserializer() {
        return deserializer;
    }
}
