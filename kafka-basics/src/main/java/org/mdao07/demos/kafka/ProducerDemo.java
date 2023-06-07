package org.mdao07.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

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
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"3f27zjaSlpULADekW5YWxt\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIzZjI3emphU2xwVUxBRGVrVzVZV3h0Iiwib3JnYW5pemF0aW9uSWQiOjczNjMyLCJ1c2VySWQiOjg1NjIwLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyZDJkZWQ4YS1lOGM0LTRhMjUtYjBjNS03YzhlODlmZjE4NjcifX0.P4DxRlFnr9J9UQGzogk7w59NIECIAZGjnXkzDlotE5Q\";");
        properties.setProperty("sasl.mechanism", "PLAIN");



        // localhost
        //properties.setProperty("bootstrap.severs", "127.0.0.1:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create kafka producer
        log.info("Before producer");

        //ProducerConfig config = new ProducerConfig(properties);
        var producer = new KafkaProducer<String, String>(properties);

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