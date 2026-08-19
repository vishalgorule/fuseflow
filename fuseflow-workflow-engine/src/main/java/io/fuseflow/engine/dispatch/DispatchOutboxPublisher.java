package io.fuseflow.engine.dispatch;

import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.config.ReliabilityProperties;
import io.fuseflow.engine.registry.PoolRoutingTable;
import io.fuseflow.engine.repository.DispatchOutboxRepository;
import io.fuseflow.engine.repository.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The other half of the dispatch outbox (post-Phase 7 hardening): publishes PENDING outbox
 * rows — the durable hand-off written by {@code Scheduler}/{@code RetryScheduler} in the same
 * transaction that made the activity SCHEDULED — so a crash between DB commit and Kafka publish
 * can never lose a task.
 *
 * <p>The routing check lives HERE, not just at schedule time: an unroutable row (no ONLINE pool
 * advertises the activity) stays PENDING and is retried each poll — the moment a capable pool
 * joins, the next tick publishes it. It never touches the start-timeout or retry clocks, so a
 * temporarily-absent pool costs no attempts and cannot fail the workflow. The
 * {@code ActivityUnroutable} event is appended once per row (dedup via the outbox's
 * {@code error} column), not on every poll.
 *
 * <p>At-least-once: the row is marked PUBLISHED after the dispatch is handed to the dispatcher;
 * a crash in between re-publishes (idempotent by {@code (execution, task, attempt)}), and a
 * PUBLISHED message lost to a broker outage is re-dispatched by boot-time recovery.
 */
@Component
public class DispatchOutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(DispatchOutboxPublisher.class);

    private final DispatchOutboxRepository outboxRepository;
    private final PoolRoutingTable routingTable;
    private final TaskDispatcher taskDispatcher;
    private final EventStore eventStore;
    private final ReliabilityProperties properties;

    public DispatchOutboxPublisher(DispatchOutboxRepository outboxRepository,
                                   PoolRoutingTable routingTable,
                                   TaskDispatcher taskDispatcher,
                                   EventStore eventStore,
                                   ReliabilityProperties properties) {
        this.outboxRepository = outboxRepository;
        this.routingTable = routingTable;
        this.taskDispatcher = taskDispatcher;
        this.eventStore = eventStore;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${fuseflow.engine.outbox.poll-interval:1s}")
    public void publishPending() {
        List<DispatchOutboxRepository.Entry> pending =
                outboxRepository.findPending(properties.getOutbox().getPollBatchSize());
        for (DispatchOutboxRepository.Entry entry : pending) {
            if (routingTable.resolveTopic(entry.activityName(), entry.taskId()).isEmpty()) {
                markUnroutable(entry);
                continue;
            }
            taskDispatcher.dispatch(new ActivityTask(entry.workflowExecutionId(), entry.taskId(),
                    entry.activityName(), entry.input(), entry.attempt()));
            outboxRepository.markPublished(entry.id());
            log.debug("Published outbox dispatch of activity {} for execution {} (attempt {})",
                    entry.activityName(), entry.workflowExecutionId(), entry.attempt());
        }
    }

    private void markUnroutable(DispatchOutboxRepository.Entry entry) {
        String reason = "no ONLINE pool advertises activity '" + entry.activityName() + "'";
        if (outboxRepository.markUnroutable(entry.id(), reason)) {
            eventStore.append(entry.workflowExecutionId(), "ActivityUnroutable", Map.of(
                    "taskId", entry.taskId(),
                    "activityName", entry.activityName(),
                    "reason", reason));
            log.warn("Activity {} of execution {} has no routable pool — waiting in the outbox (no retry clock)",
                    entry.activityName(), entry.workflowExecutionId());
        }
    }
}
