package io.fuseflow.registry.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the registry's Kafka topics so they are created idempotently on boot (KafkaAdmin;
 * non-fatal when the broker is unreachable). The registry only produces {@code worker-events}.
 */
@Configuration
@ConditionalOnProperty(name = "fuseflow.registry.events-enabled", havingValue = "true", matchIfMissing = true)
public class RegistryMessagingConfig {

    @Bean
    public NewTopic workerEventsTopic(
            @Value("${fuseflow.kafka.topic.worker-events}") String name,
            @Value("${fuseflow.kafka.topics.partitions:1}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas((short) 1).build();
    }
}
