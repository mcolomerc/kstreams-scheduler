package org.example;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class ScheduledProcessor extends AbstractProcessor<String, CustomRecord> {
    private ProcessorContext context;
    private KeyValueStore<String, CustomRecord> stateStore;
    private static Logger logger =  LoggerFactory.getLogger(ScheduledProcessor.class.getName());

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = (KeyValueStore<String, CustomRecord>) context.getStateStore("in-store");
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
            @Override
            public void punctuate(long timestamp) {
                processScheduledMessages(timestamp);
            }
        });
    }

    @Override
    public void process( String key, CustomRecord record) {
        stateStore.put(key, record);
    }

    private void processScheduledMessages(long currentTimestamp) {
        try (KeyValueIterator<String, CustomRecord> iterator = stateStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, CustomRecord> entry = iterator.next();
                if (entry.value.getScheduledTimestamp() <= currentTimestamp) {
                    // Forward the message to the next topic
                    logger.info ("Forwarding message: " + entry.value);
                    context.forward(entry.key, entry.value);
                    stateStore.delete(entry.key);
                }
            }
        }
    }

    @Override
    public void close() {
        // Close any resources if needed
    }
}

