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
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// source: https://www.conduktor.io/kafka/java-consumer-in-threads/
public class ConsumerInThreadsDemo {

    private static Logger log = LoggerFactory.getLogger(ConsumerInThreadsDemo.class);

    static class ConsumerWorker implements Runnable {

        static final Duration pollTimeOut = Duration.ofMillis(100L);

        private KafkaConsumer<String, String> consumer;
        private CountDownLatch countDownLatch;

        ConsumerWorker(KafkaConsumer<String, String> consumer, CountDownLatch countDownLatch) {
            this.consumer = consumer;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            String name = Thread.currentThread().getName();
            int count = 0;

            try {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    /*
                    for (ConsumerRecord<String, String> record : records) {
                        ++count;
                        log.info("Thread: {}, Key: {}, Value: {}", name, record.key(), record.value());
                        //log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }

                     */

                    count += records.count();

                }
            } catch (WakeupException e) {
                log.info("Consumer on thread {} woke up", name);
            } finally {
                log.info("Consumer on thread {} read {} messages", name, count);
                countDownLatch.countDown();
                consumer.close();
            }
        }

        public void shutDown() {
            consumer.wakeup();
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"7fl0ltLZJNASX0xWE04zwX\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI3ZmwwbHRMWkpOQVNYMHhXRTA0endYIiwib3JnYW5pemF0aW9uSWQiOjczOTYzLCJ1c2VySWQiOjg2MDIxLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJkZmRiYzA4NC0xMWQ1LTQ4MWQtODVjNy1lMDc4YWVhZTlmOTEifX0.eu8bInqKhOoJVjR-zRXV5jtUxzJGbykN0RtWIeE2V0k\";");
        properties.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "threads-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }

    private static void addCustomShutdownHook(Collection<KafkaConsumer<String, String>> consumers) {
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown detected, exiting by calling consumer.wakeup()...");
            consumers.forEach(KafkaConsumer::wakeup);

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    public static void main(String[] args) {
        int numberOfWorkers = 3;
        Properties properties = getProperties();
        CountDownLatch countDownLatch = new CountDownLatch(numberOfWorkers);
        List<String> topics = List.of("threads-demo");

        var consumersList = Stream.generate(() -> new KafkaConsumer<String, String>(properties))
                .limit(numberOfWorkers)
                .collect(Collectors.toList());

        consumersList.forEach(c -> c.subscribe(topics));
        addCustomShutdownHook(consumersList);

        var workersList = consumersList.stream()
                .map(consumer -> new ConsumerWorker(consumer, countDownLatch))
                .collect(Collectors.toList());

        workersList.forEach(worker -> new Thread(worker).start());

        try {
            countDownLatch.await(15L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            workersList.forEach(ConsumerWorker::shutDown);
        }

        log.info("Main thread exit");
    }
}
