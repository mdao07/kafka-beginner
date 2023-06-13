package org.mdao07.demos.kafka.advanced;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

// source: https://www.conduktor.io/kafka/java-consumer-rebalance-listener/
public class RebalanceListenerImpl implements ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(RebalanceListenerImpl.class);

    private KafkaConsumer<String, String> consumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    public RebalanceListenerImpl(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
        currentOffsets = new HashMap<>();
    }

    public void addOffsetToTrack(String topic, int partition, long offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset + 1, null);

        currentOffsets.put(topicPartition, offsetAndMetadata);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("onPartitionsRevoked callback");
        log.info("Committing offsets: " + currentOffsets);

        // block until the offsets are successfully committed
        consumer.commitSync(currentOffsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("onPartitionsAssigned callback");
    }

    // used when the consumer is shutdown gracefully
    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }
}
