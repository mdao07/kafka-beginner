package org.mdao07.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeysDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeysDemo.class.getSimpleName());

    public static void main(String[] args) {
        // Kafka cluster properties
        ProducerProperties producerProperties = new ProducerProperties(log);
        producerProperties.setCommonProperties();

        // create kafka producer
        var producer = new KafkaProducer<String, String>(producerProperties.getProperties());

        // create producer record
        String topic = "demo_java";

        for (int j = 0; j < 2; ++j) {

            for (int i = 0; i < 10; ++i) {
                String key = "id_" + i;
                String value = "msg_" + System.currentTimeMillis();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // send data with a callback
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        var sb = new StringBuilder();

                        sb.append(String.format("Key: %s, ", key));
                        sb.append(String.format("Partition: %d, ", metadata.partition()));
                        sb.append(String.format("Offset: %d, ", metadata.offset()));
                        sb.append(String.format("Timestamp: %d%n", metadata.timestamp()));

                        log.info("Record sent, Metadata received");
                        log.info(sb.toString());
                    } else {
                        log.error("Error while producing", exception);
                    }
                });
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // tell the producer to send all data and block until done (synchronous)
        // this is called during producer.close() too
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
