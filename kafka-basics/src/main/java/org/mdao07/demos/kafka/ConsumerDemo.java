package org.mdao07.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        String topic = "demo_java";
        String groupId = "my-java-application";

        // Kafka cluster properties
        ConsumerProperties consumerProperties = new ConsumerProperties(log);
        consumerProperties.setCommonProperties();
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties.getProperties());

        // subscribe to topic
        consumer.subscribe(List.of(topic));

        // poll for data
        while(true) {
            log.info("Polling...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));

            records.forEach(r -> {
                log.info("Key: " + r.key() + ", Value: " + r.value());
                log.info("Partition: " + r.partition() + ", Offset: " + r.offset());
            });
        }

        /*
        NOTES
        Consumers also batch data, they can receive up to 1MB of data of the broker each time

        Consumers take some time to (re)join a consumer group before they can get the data if any.
         */
    }
}