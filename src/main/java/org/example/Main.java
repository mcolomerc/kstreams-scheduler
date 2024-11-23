package org.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;
import org.example.serde.CustomRecordSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Main {
    private static Logger logger =  LoggerFactory.getLogger(Main.class.getName());
    public static final String IN_TOPIC  = "in-topic";
    public static final String OUT_TOPIC  = "out-topic";
    protected static Properties properties;
    protected static KafkaStreams kafkaStreams;
    private static final Serde<CustomRecord> serde = new CustomRecordSerdes();

    public static void main(String[] args) {
    logger.info ("Starting Stream App...");
        try {
        properties = new Properties();
        if (args.length > 0) {
            properties.load(new FileInputStream(args[0]));
        } else {
            properties.load(Main.class.getResourceAsStream("/confluent.properties"));
        }
        logger.info(String.valueOf(properties));
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
            properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                    CreatedAtTimestampExtractor.class.getName());

            properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "scheduler-app");

            StreamsBuilder builder = new StreamsBuilder();
            buildTopology(builder);
            Topology topology = builder.build();
            logger.info(topology.describe().toString());

            kafkaStreams = new KafkaStreams(topology, properties);
            kafkaStreams.setUncaughtExceptionHandler((e) -> {
                logger.error(null, e);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            });
            kafkaStreams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void buildTopology(StreamsBuilder builder) throws ExecutionException, InterruptedException {
        logger.info ("Building topology...");

        KStream<String, CustomRecord> inStream = builder.stream(IN_TOPIC, Consumed.with(Serdes.String(), serde));

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("in-store"),
                Serdes.String(),
                serde));

        inStream.process(ScheduledProcessor::new, "in-store");

        inStream.to(OUT_TOPIC);
    }
}