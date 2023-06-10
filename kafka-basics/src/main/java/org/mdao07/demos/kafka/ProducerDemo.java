package org.mdao07.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

    /*
     * 1) create producer properties
     * 2) create the producer
     * 3) send date
     * 4) flush and close the producer
     */
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        // Kafka cluster properties
        ProducerProperties producerProperties = new ProducerProperties(log);
        producerProperties.setCommonProperties();

        // create kafka producer
        var producer = new KafkaProducer<String, String>(producerProperties.getProperties());

        // create producer record
        var record = new ProducerRecord<String, String>("demo_java", "java msg 2");

        // send data
        producer.send(record);

        // tell the producer to send all data and block until done (synchronous)
        // this is called during producer.close() too
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}