package org.mdao07.demos.kafka.advanced;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

// source: https://www.conduktor.io/kafka/java-consumer-rebalance-listener/
public class RebalanceListenerDemo {

    private static Logger log = LoggerFactory.getLogger(RebalanceListenerDemo.class);

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"7fl0ltLZJNASX0xWE04zwX\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI3ZmwwbHRMWkpOQVNYMHhXRTA0endYIiwib3JnYW5pemF0aW9uSWQiOjczOTYzLCJ1c2VySWQiOjg2MDIxLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJkZmRiYzA4NC0xMWQ1LTQ4MWQtODVjNy1lMDc4YWVhZTlmOTEifX0.eu8bInqKhOoJVjR-zRXV5jtUxzJGbykN0RtWIeE2V0k\";");
        properties.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "rebalance-group-1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }

    private static void addCustomShutdownHook(KafkaConsumer<String, String> consumer) {
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown detected, exiting by calling consumer.wakeup()...");
            consumer.wakeup();

            try {
                mainThread.join();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    public static void main(String[] args) {
        log.info("Consumer with Rebalance listener");

        Properties properties = getProperties();

        String topic = "rebalance-listener";

        // create consumer with its associated rebalance listener
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        RebalanceListenerImpl listener = new RebalanceListenerImpl(consumer);
        addCustomShutdownHook(consumer);

        try {
            // subscribe consumer to the topic, note the listener is passed too
            consumer.subscribe(Arrays.asList(topic), listener);

            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                records.forEach(record -> {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                    // manually tracking offsets with the rebalance listener
                    listener.addOffsetToTrack(record.topic(), record.partition(), record.offset());
                });

                // commit asynchronously to not block until the next poll() call
                // since all data has been processed
                consumer.commitAsync();
            }

        } catch (WakeupException e) {
            log.info("WakeupException! This is expected");
        } catch (Exception e) {
            log.error("UnexpectedException", e);
        } finally {
            try {
                consumer.commitSync(listener.getCurrentOffsets());
            } finally {
                consumer.close();
                log.info("Consumer gracefully closed");
            }
        }
    }
}
