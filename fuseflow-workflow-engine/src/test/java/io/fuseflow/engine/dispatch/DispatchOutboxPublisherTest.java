package io.fuseflow.engine.dispatch;

import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.config.ReliabilityProperties;
import io.fuseflow.engine.registry.PoolRoutingTable;
import io.fuseflow.engine.repository.DispatchOutboxRepository;
import io.fuseflow.engine.repository.EventStore;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DispatchOutboxPublisher} (post-Phase 7 hardening): the outbox poller
 * publishes PENDING dispatch rows when the routing table has a capable ONLINE pool, marks them
 * PUBLISHED, and — when unroutable — leaves them PENDING (waiting for a pool, NOT burning the
 * start-timeout/retry clock) appending the {@code ActivityUnroutable} event exactly once.
 */
class DispatchOutboxPublisherTest {

    private final DispatchOutboxRepository outboxRepository = mock(DispatchOutboxRepository.class);
    private final PoolRoutingTable routingTable = mock(PoolRoutingTable.class);
    private final TaskDispatcher taskDispatcher = mock(TaskDispatcher.class);
    private final EventStore eventStore = mock(EventStore.class);
    private final ReliabilityProperties properties = new ReliabilityProperties();
    private final DispatchOutboxPublisher publisher =
            new DispatchOutboxPublisher(outboxRepository, routingTable, taskDispatcher, eventStore, properties);

    private static DispatchOutboxRepository.Entry entry(String taskId, String activity, int attempt, String status) {
        return new DispatchOutboxRepository.Entry(UUID.randomUUID(), UUID.randomUUID(), taskId, activity,
                "{\"k\":1}", attempt, status, null, Instant.now(), null);
    }

    @Test
    void publishesRoutablePendingRowsAndMarksThemPublished() {
        DispatchOutboxRepository.Entry pending = entry("a", "resizeImage", 1, "PENDING");
        when(outboxRepository.findPending(properties.getOutbox().getPollBatchSize()))
                .thenReturn(List.of(pending));
        when(routingTable.resolveTopic("resizeImage", "a")).thenReturn(Optional.of("fuseflow-pool.media"));

        publisher.publishPending();

        ArgumentCaptor<ActivityTask> task = ArgumentCaptor.forClass(ActivityTask.class);
        verify(taskDispatcher).dispatch(task.capture());
        assertThat(task.getValue().executionId()).isEqualTo(pending.workflowExecutionId());
        assertThat(task.getValue().taskId()).isEqualTo("a");
        assertThat(task.getValue().activityName()).isEqualTo("resizeImage");
        assertThat(task.getValue().attempt()).isEqualTo(1);
        assertThat(task.getValue().input()).isEqualTo("{\"k\":1}");
        verify(outboxRepository).markPublished(pending.id());
        verify(eventStore, never()).append(any(), eq("ActivityUnroutable"), any());
    }

    @Test
    void leavesUnroutableRowsPendingAndAppendsEventOnce() {
        DispatchOutboxRepository.Entry pending = entry("a", "resizeImage", 1, "PENDING");
        when(outboxRepository.findPending(properties.getOutbox().getPollBatchSize()))
                .thenReturn(List.of(pending));
        when(routingTable.resolveTopic("resizeImage", "a")).thenReturn(Optional.empty());
        // First sighting: markUnroutable returns true → the event is appended once.
        when(outboxRepository.markUnroutable(pending.id(), "no ONLINE pool advertises activity 'resizeImage'"))
                .thenReturn(true);

        publisher.publishPending();
        verify(eventStore).append(eq(pending.workflowExecutionId()), eq("ActivityUnroutable"), any());
        verify(taskDispatcher, never()).dispatch(any());
        verify(outboxRepository, never()).markPublished(pending.id());

        // Subsequent polls while the pool is still away: markUnroutable returns false → the
        // history is not spammed; the row stays PENDING (still no retry clock, no dispatch).
        when(outboxRepository.markUnroutable(pending.id(), "no ONLINE pool advertises activity 'resizeImage'"))
                .thenReturn(false);
        publisher.publishPending();
        verify(eventStore, org.mockito.Mockito.times(1)).append(any(), eq("ActivityUnroutable"), any());
    }

    @Test
    void doesNothingWhenOutboxIsEmpty() {
        when(outboxRepository.findPending(properties.getOutbox().getPollBatchSize())).thenReturn(List.of());
        publisher.publishPending();
        verify(taskDispatcher, never()).dispatch(any());
    }
}
