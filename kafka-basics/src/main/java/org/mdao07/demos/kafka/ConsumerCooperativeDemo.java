package org.mdao07.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class ConsumerCooperativeDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCooperativeDemo.class.getSimpleName());

    public static void main(String[] args) {
        String topic = "demo_java";
        String groupId = "my-java-application";

        // Kafka cluster properties
        ConsumerProperties consumerProperties = new ConsumerProperties(log);
        consumerProperties.setCommonProperties();
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        /*
        There are 2 types:

        * Eager Rebalance
            - All consumers stop and give up their membership of partitions
            - During a short period of time, the entire consumer group stop processing (Stop the world event)
            - Consumers rejoin the group and get a new partition assignment (not necessarily
              the same partition they had before)

            Examples
            * RangeAssignor: assigns partitions on a per-topic basis (can lead to imbalance)
            * RoundRobin: assigns partitions across all topics in round-robin fashion (optimal balance)
            * StickyAssignor: balanced like RoundRobin, and then minimizes partition movements when
                              the consumer joins/leaves the group

        *  Cooperative Rebalance (Incremental Rebalance)
            - Reassigning a small subset of the partitions from one consumer to another
            - Consumers that don't have reassigned partitions can continue processing uninterrupted
            - Can go through several iterations to find a "stable" assigment (hence "incremental")
            - Avoids the "stop the world" event where all consumer stop processing data

            Examples
            * CooperativeStickyAssignor: the rebalance strategy is identical to StickyAssignor, but
                                         it supports cooperative rebalances and therefore consumers
                                         can keep on consuming from the topic.


         Strategies in applications
            * For Kafka Consumer by default is [RangeAssignor, CooperativeStickyAssignor]
            * For Kafka Connect cooperative rebalance is enabled by default.
            * For Kafka Streams cooperative rebalance is enabled by default using StreamsPartitionAssignor.

        */
        consumerProperties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        /*
        Static Group Membership

        - By default, when a consumer leaves a group, its partitions are revoked and reassigned.
          If it joins back, it will have a new "member ID" and new partitions assigned.

        - By specifying "group.instance.id" it makes the consumer a static member.
          This means that, upon the consumer leaving, it has a limit time (specified in "session.timeout.ms")
          to re-join and get back its partitions without triggering a re-balance.
          If the limit time is exceeded, its partitions need be reassigned, triggering a re-balance.

          This is helpful when consumers maintain local state and cache (avoids re-building the cache).
         */

        // properties.setProperty("group.instance.id", "..."); // strategy for static assignments

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
         */

    }
}