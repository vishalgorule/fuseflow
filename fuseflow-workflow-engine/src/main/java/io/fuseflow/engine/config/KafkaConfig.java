package io.fuseflow.engine.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

/**
 * Declares the engine's Kafka topics so they are created idempotently on boot (KafkaAdmin, via
 * Spring Boot's auto-configuration; creation is non-fatal when the broker is unreachable).
 * Topic names are configurable ({@code fuseflow.kafka.topic.*}); partition/replica counts are
 * dev-friendly defaults (a single-broker KRaft setup).
 */
@Configuration
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "kafka", matchIfMissing = true)
public class KafkaConfig {

    private static final short DEFAULT_REPLICAS = 1;

    @Bean
    public NewTopic activityResultsTopic(
            @Value("${fuseflow.kafka.topic.activity-results}") String name,
            @Value("${fuseflow.kafka.topics.partitions:1}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(DEFAULT_REPLICAS).build();
    }

    @Bean
    public NewTopic workflowEventsTopic(
            @Value("${fuseflow.kafka.topic.workflow-events}") String name,
            @Value("${fuseflow.kafka.topics.partitions:1}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(DEFAULT_REPLICAS).build();
    }

    @Bean
    public NewTopic deadLetterTopic(
            @Value("${fuseflow.kafka.topic.dead-letter}") String name,
            @Value("${fuseflow.kafka.topics.partitions:1}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(DEFAULT_REPLICAS).build();
    }

    @Bean
    public NewTopic workerEventsTopic(
            @Value("${fuseflow.kafka.topic.worker-events}") String name,
            @Value("${fuseflow.kafka.topics.partitions:1}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(DEFAULT_REPLICAS).build();
    }

    /**
     * Admin client for dynamic pool-topic provisioning (Phase 5): pool topics are created when
     * a pool first appears in the routing table, sized from the pool's declared concurrency.
     */
    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }
}
