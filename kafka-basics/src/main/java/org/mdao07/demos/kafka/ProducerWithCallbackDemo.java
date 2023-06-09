package org.mdao07.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallbackDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallbackDemo.class.getSimpleName());

    public static void main(String[] args) {
        // Kafka cluster properties
        ProducerProperties producerProperties = new ProducerProperties(log);
        producerProperties.setCommonProperties();
        producerProperties.setProperty("batch.size", "400");

        //Not recommended
        //producerProperties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // create kafka producer
        log.info("Before producer");

        var producer = new KafkaProducer<String, String>(producerProperties.getProperties());

        Callback callback = (metadata, exception) -> {
            if (exception == null) {
                var sb = new StringBuilder();

                sb.append(String.format("Topic: %s, ", metadata.topic()));
                sb.append(String.format("Partition: %d, ", metadata.partition()));
                sb.append(String.format("Offset: %d, ", metadata.offset()));
                sb.append(String.format("Timestamp: %d%n", metadata.timestamp()));

                log.info("Record sent, Metadata received");
                log.info(sb.toString());
            } else {
                log.error("Error while producing", exception);
            }
        };

        // create producer records
        for (int batch = 0; batch < 5; ++batch) {

            for (int i = 0; i < 10; ++i) {
                ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "java callback " + System.currentTimeMillis());

                // send data with a callback
                producer.send(record, callback);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        /*
        Property "partitioner.class" = null
        This setting will make the producer use the DefaultPartitioner, which is the Sticky Partitioner.

        STICKY PARTITIONER
        If many messages are sent too quickly, the producer will send messages in batches
        to one partition of the topic. This is made for efficiency, as is faster to send the
        messages this way, compared with RoundRobin the partitions.
         */

        // tell the producer to send all data and block until done (synchronous)
        // this is called during producer.close() too
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
