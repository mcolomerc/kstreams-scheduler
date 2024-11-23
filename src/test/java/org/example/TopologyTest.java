package org.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.serde.CustomRecordSerdes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;


public class TopologyTest {

    static Logger logger = LoggerFactory.getLogger(TopologyTest.class.getName());

    static final String InputTopic1 = "in1";
    static final String OutputTopic = "out";

    @Test
    public void shouldNotGenerateOutput() {
        final Serde<CustomRecord> serde = new CustomRecordSerdes();

        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(createTopology(), getConfig(), Instant.now())) {

            final TestInputTopic<String, CustomRecord> input = topologyTestDriver
                    .createInputTopic(InputTopic1, new StringSerializer(), serde.serializer());

            final TestOutputTopic<String, CustomRecord> output = topologyTestDriver
                    .createOutputTopic(OutputTopic, new StringDeserializer(), serde.deserializer());

            CustomRecord record = new CustomRecord("1",  "should not be forwarded",
                    Instant.now().plusMillis(5*1000).toEpochMilli());

            logger.info ("Sending record: " + record);
            input.pipeInput("1", record);
            topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1));

            var actualRecords = output.readRecordsToList();
            assertThat(actualRecords).isEmpty();
        }
    }

  @Test
  public void shouldGenerateOutput() {
        final Serde<CustomRecord> serde = new CustomRecordSerdes();

        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(createTopology(), getConfig(), Instant.now())) {

            final TestInputTopic<String, CustomRecord> input = topologyTestDriver
                    .createInputTopic(InputTopic1, new StringSerializer(), serde.serializer());

            final TestOutputTopic<String, CustomRecord> output = topologyTestDriver
                    .createOutputTopic(OutputTopic, new StringDeserializer(), serde.deserializer());

            CustomRecord record = new CustomRecord("1",  "should be forwarded",
                    Instant.now().plusMillis(5*1000).toEpochMilli());

            logger.info ("Sending record: " + record);
            input.pipeInput("1", record);
            topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(10));

            var actualRecords = output.readRecordsToList();
            assertEquals(actualRecords.get(0).getValue(), record);
        }
    }


    private Topology createTopology() {
        final Serde<CustomRecord> serde = new CustomRecordSerdes();
        Topology topology = new Topology();
        StoreBuilder<KeyValueStore<String, CustomRecord>> storeSupplier = Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("in-store"),
                        Serdes.String(),
                        new CustomRecordSerdes())
                .withLoggingDisabled();

        topology.addSource("Source", InputTopic1)
                .addProcessor("Process", () -> new ScheduledProcessor(), "Source")
                .addStateStore(storeSupplier, "Process")
                .addSink("Sink", OutputTopic, "Process");

        return topology;
    }

    private Properties getConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ""); // empty string
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/aggregated-values");
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-wallclock-app");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomRecordSerdes.class.getName());
        return config;
    }


}
