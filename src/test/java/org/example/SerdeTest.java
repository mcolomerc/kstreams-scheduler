package org.example;

import org.example.serde.CustomRecordSerdes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SerdeTest {

    @Test
    public void testSerde() {
        CustomRecord record = new CustomRecord("1", "value", 1234567890);
        CustomRecordSerdes serdes = new CustomRecordSerdes();
        byte[] serializedRecord = serdes.serializer().serialize("topic", record);
        System.out.println("Serialized Record: " + new String(serializedRecord));

        CustomRecord deserializedRecord = serdes.deserializer().deserialize("topic", serializedRecord);
        assertEquals(record, deserializedRecord);
    }
}
