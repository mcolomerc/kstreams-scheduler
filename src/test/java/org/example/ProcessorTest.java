package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.example.serde.CustomRecordSerdes;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.Properties;

import static org.junit.Assert.*;

public class ProcessorTest {
    static Logger logger = LoggerFactory.getLogger(ProcessorTest.class.getName());

    ScheduledProcessor processorUnderTest;
    Properties config;
    MockProcessorContext context;
    KeyValueStore<String, CustomRecord> store;

    @Before
    public void setup() {
        processorUnderTest = new ScheduledProcessor();
        config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        context = new MockProcessorContext(config);
        processorUnderTest.init(context);

        store = Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("in-store"),
                        Serdes.String(),
                        new CustomRecordSerdes())
                .withLoggingDisabled()
                .build();

        store.init(context, store);
        context.register(store, null);
        context.setTopic("topicName");
        context.setPartition(0);
        context.setOffset(0L);
        context.setTimestamp(Instant.now().toEpochMilli());
        processorUnderTest.init(context);
    }

    @Test
    public void testFutureRecords() {
        CustomRecord record = new CustomRecord("1",  "future message should not be forwarded",
                Instant.now().plusMillis(5*1000).toEpochMilli()); //scheduledTs > currentTs

        // Adds the record to the store
        logger.info ("Producing record: " + record);
        processorUnderTest.process("1", record);

        final MockProcessorContext.CapturedPunctuator capturedPunctuator = context.scheduledPunctuators().get(0);
        final Punctuator punctuator = capturedPunctuator.getPunctuator();
        punctuator.punctuate(Instant.now().plusMillis(1*1000).toEpochMilli()); //scheduledTs > currentTs

        final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();
        assertFalse(forwarded.hasNext());

        // Validate record remains in Store (size should be 1)
        long storeSize = store.approximateNumEntries();
        assertEquals(1, storeSize);

        context.resetForwards();
        assertEquals(0, context.forwarded().size());
    }

    @Test
    public void testActualRecords() {
        CustomRecord record = new CustomRecord("1",  "past message should be forwarded",
                Instant.now().plusMillis(2*1000).toEpochMilli()); //scheduledTs

        // Adds the record to the store
        logger.info ("Producing record: " + record);
        processorUnderTest.process("1", record);

        final MockProcessorContext.CapturedPunctuator capturedPunctuator = context.scheduledPunctuators().get(0);
        final Punctuator punctuator = capturedPunctuator.getPunctuator();
        punctuator.punctuate(Instant.now().plusMillis(3*1000).toEpochMilli()); //scheduledTs < currentTs

        // It was Forwarded
        final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();
        assertTrue(forwarded.hasNext());
        assertEquals(record, forwarded.next().keyValue().value);

        // Record removed from the store
        long storeSize = store.approximateNumEntries();
        assertEquals(0, storeSize);

        context.resetForwards();
        assertEquals(0, context.forwarded().size());

    }

}
