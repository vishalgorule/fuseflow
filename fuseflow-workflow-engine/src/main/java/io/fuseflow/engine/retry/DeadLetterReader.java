package io.fuseflow.engine.retry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Read-only inspection of the {@code dead-letter} topic for the {@code GET /api/v1/dead-letters}
 * endpoint (Phase 7, plan §9 task 6). Each call uses a dedicated, non-committing consumer that
 * seeks to the beginning, so the endpoint always shows the full DLQ history regardless of who
 * has read it before — no offsets are moved.
 */
@Component
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "kafka", matchIfMissing = true)
public class DeadLetterReader {

    private final String bootstrapServers;
    private final String topic;
    private final ObjectMapper objectMapper;

    public DeadLetterReader(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                            @Value("${fuseflow.kafka.topic.dead-letter}") String topic,
                            ObjectMapper objectMapper) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.objectMapper = objectMapper;
    }

    /** Returns up to {@code limit} dead-letter messages from the beginning of the topic. */
    public List<Map<String, Object>> read(int limit) {
        if (limit <= 0) {
            return List.of();
        }
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "fuseflow-dead-letter-inspector",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                return List.of(); // topic not created yet — nothing dead-lettered
            }
            consumer.assign(partitions.stream()
                    .map(partition -> new TopicPartition(topic, partition.partition()))
                    .toList());
            consumer.seekToBeginning(consumer.assignment());

            List<Map<String, Object>> messages = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 2_000;
            while (messages.size() < limit && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    messages.add(parse(record.value()));
                    if (messages.size() >= limit) {
                        break;
                    }
                }
            }
            return messages;
        }
    }

    private Map<String, Object> parse(String value) {
        try {
            return objectMapper.readValue(value, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception ex) {
            return Map.of("malformed", value == null ? "" : value);
        }
    }
}
