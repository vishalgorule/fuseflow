package io.fuseflow.engine.registry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code worker-events} (Phase 5): every worker state change — registered,
 * deregistered, offline — invalidates the {@link PoolRoutingTable}, which is re-seeded from the
 * registry. Pool membership and liveness are what routing depends on, so a full re-seed on each
 * event is the simplest correct refresh; it never touches the dispatch hot path.
 */
@Component
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "kafka", matchIfMissing = true)
public class WorkerEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(WorkerEventConsumer.class);

    private final PoolRoutingService routingService;

    public WorkerEventConsumer(PoolRoutingService routingService) {
        this.routingService = routingService;
    }

    @KafkaListener(topics = "${fuseflow.kafka.topic.worker-events}",
            groupId = "${fuseflow.engine.worker-events-group:fuseflow-engine-events}")
    public void onWorkerEvent(ConsumerRecord<String, String> record) {
        log.debug("Worker event on {}: refreshing pool routing", record.value());
        routingService.refresh();
    }
}
