package org.mdao07.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class ConsumerWithShutdownDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithShutdownDemo.class.getSimpleName());

    public static void main(String[] args) {
        String topic = "demo_java";
        String groupId = "my-java-application";

        // Kafka cluster properties
        ConsumerProperties consumerProperties = new ConsumerProperties(log);
        consumerProperties.setCommonProperties();
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties.getProperties());

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown detected, exiting by calling consumer.wakeup()...");
            consumer.wakeup();

            // join the main thread
            try {
                mainThread.join();
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }));

        // subscribe to topic
        consumer.subscribe(List.of(topic));

        // poll for data
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));

                records.forEach(r -> {
                    log.info("Key: " + r.key() + ", Value: " + r.value());
                    log.info("Partition: " + r.partition() + ", Offset: " + r.offset());
                });
            }
        } catch (WakeupException e) {
            log.info("Shutting down consumer");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer...");
        } finally {
            consumer.close();
            log.info("Consumer gracefully shutdown");
        }

        /*
        NOTES
        Consumers also batch data, they can receive up to 1MB of data of the broker each time

        Consumers take some time to (re)join a consumer group before they can get the data if any.

        Auto Offset Commit Behavior

        - In the Java Consumer API, offsets are committed regularly
        - Make sure you read and process your messages between each poll, so you guarantee an at-least-once reading scenario.
        - Offsets are committed after .poll() is called, if the property "enable.auto.commit" = true and the property
          "auto.commit.interval.ms" time has elapsed.
          Example: auto.commit.interval.ms=5000 and enable.auto.commit=true

        If you have to disable enable.auto.commit, and/or do processing in a separate thread, you have to call manually
        .commitSync() or .commitAsync() from time-to-time with the correct offsets.
         */

    }
}