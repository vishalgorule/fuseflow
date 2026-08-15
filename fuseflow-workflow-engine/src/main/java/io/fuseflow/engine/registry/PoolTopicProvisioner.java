package io.fuseflow.engine.registry;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Idempotent pool-topic provisioning (Phase 5): creates {@code fuseflow-pool.<poolName>} with
 * {@code min(declared concurrency, cap)} partitions when a pool first appears in the routing
 * table. Partitions set the parallelism ceiling for the pool's consumer group
 * ({@code min(instances, partitions)}); the cap prevents a mis-declared concurrency from
 * over-provisioning. Existing topics are left untouched (create is a no-op) — partitions only
 * grow by explicit operator action.
 */
@Component
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "kafka", matchIfMissing = true)
public class PoolTopicProvisioner {

    private static final Logger log = LoggerFactory.getLogger(PoolTopicProvisioner.class);

    private final AdminClient adminClient;
    private final String topicPrefix;
    private final int partitionCap;
    private final Set<String> provisioned = ConcurrentHashMap.newKeySet();

    public PoolTopicProvisioner(AdminClient adminClient,
                                @Value("${fuseflow.kafka.topic.pool-prefix:fuseflow-pool}") String topicPrefix,
                                @Value("${fuseflow.engine.pool-topic.partition-cap:16}") int partitionCap) {
        this.adminClient = adminClient;
        this.topicPrefix = topicPrefix;
        this.partitionCap = partitionCap;
    }

    /**
     * Creates the pool's topic once per JVM (idempotent across refreshes and restarts). If the
     * topic already exists — e.g. the broker auto-created it with 1 partition while workers
     * subscribed before the engine — it is raised to the target partition count (partitions can
     * only grow).
     */
    public void ensure(String poolName, int concurrency) {
        String topic = topicPrefix + "." + poolName;
        if (!provisioned.add(topic)) {
            return;
        }
        int partitions = Math.max(1, Math.min(Math.max(1, concurrency), partitionCap));
        try {
            adminClient.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all()
                    .whenComplete((ignored, ex) -> {
                        if (ex == null) {
                            log.info("Provisioned pool topic '{}' with {} partition(s)", topic, partitions);
                        } else if (isAlreadyExists(ex)) {
                            raisePartitions(topic, partitions);
                        } else {
                            log.warn("Failed to provision pool topic '{}': {}", topic, rootMessage(ex));
                        }
                    });
        } catch (Exception ex) {
            log.warn("Failed to provision pool topic '{}': {}", topic, rootMessage(ex));
        }
    }

    /** Sizes an existing pool topic up to the target partition count (never shrinks). */
    private void raisePartitions(String topic, int target) {
        adminClient.describeTopics(List.of(topic)).allTopicNames().whenComplete((topics, ex) -> {
            if (ex != null || topics == null || !topics.containsKey(topic)) {
                log.warn("Cannot size existing pool topic '{}': {}", topic,
                        ex == null ? "missing" : rootMessage(ex));
                return;
            }
            int current = topics.get(topic).partitions().size();
            if (current >= target) {
                log.info("Pool topic '{}' already exists with {} partition(s)", topic, current);
                return;
            }
            adminClient.createPartitions(Map.of(topic, NewPartitions.increaseTo(target)))
                    .all()
                    .whenComplete((ignored, ex2) -> {
                        if (ex2 == null) {
                            log.info("Raised pool topic '{}' partitions {} -> {}", topic, current, target);
                        } else {
                            log.warn("Failed to raise partitions of pool topic '{}': {}", topic, rootMessage(ex2));
                        }
                    });
        });
    }

    private static boolean isAlreadyExists(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof TopicExistsException) {
                return true;
            }
        }
        return false;
    }

    private static String rootMessage(Throwable ex) {
        Throwable root = ex;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage() == null ? root.toString() : root.getMessage();
    }
}
