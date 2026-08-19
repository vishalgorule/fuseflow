package io.fuseflow.engine.registry;

import io.fuseflow.common.dto.WorkerResponse;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import io.fuseflow.engine.service.Scheduler;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PoolRoutingService} (Phase 5 HA fix): the routing table re-seeds from
 * the registry on refresh — the periodic {@code refreshPeriodically} cadence and every
 * worker-events message both funnel here — so a worker that was marked OFFLINE and then revived
 * by a heartbeat becomes routable again on the next refresh even if the {@code worker_online}
 * event was missed. Post-Phase 7 hardening: a refresh that <em>gains</em> a capability also
 * re-drives runnable-PENDING activities that were left unscheduled because no pool could route
 * them.
 */
class PoolRoutingServiceTest {

    private final EngineRegistryClient registryClient = mock(EngineRegistryClient.class);
    private final PoolRoutingTable routingTable = new PoolRoutingTable("fuseflow-pool");
    private final PoolTopicProvisioner topicProvisioner = mock(PoolTopicProvisioner.class);
    private final ActivityExecutionRepository activityRepository = mock(ActivityExecutionRepository.class);
    private final WorkflowExecutionRepository executionRepository = mock(WorkflowExecutionRepository.class);
    private final Scheduler scheduler = mock(Scheduler.class);
    private final PoolRoutingService service =
            new PoolRoutingService(registryClient, routingTable, topicProvisioner,
                    activityRepository, executionRepository, scheduler);

    private static WorkerResponse worker(String pool, String status, String... activities) {
        Instant now = Instant.now();
        return new WorkerResponse(UUID.randomUUID(), "host", status, List.of(activities),
                pool, 4, now, 0, now, now);
    }

    @Test
    void refreshSeedsRoutingFromTheRegistry() {
        when(registryClient.listWorkers())
                .thenReturn(List.of(worker("media", "ONLINE", "resizeImage", "watermarkImage")));

        service.refresh();

        assertThat(routingTable.resolveTopic("resizeImage", "t1")).contains("fuseflow-pool.media");
        assertThat(routingTable.poolConcurrency("media")).isEqualTo(4);
        verify(topicProvisioner).ensure("media", 4);
    }

    @Test
    void refreshRestoresRoutingAfterOfflineRevival() {
        // First view: the worker is OFFLINE (e.g. the machine paused and the OfflineDetector
        // fired) — dispatch correctly finds no routable pool.
        when(registryClient.listWorkers())
                .thenReturn(List.of(worker("media", "OFFLINE", "resizeImage")));
        service.refresh();
        assertThat(routingTable.resolveTopic("resizeImage", "t1")).isEmpty();

        // Next refresh (periodic cadence or worker_online event): the worker is ONLINE again —
        // routing must recover without any worker re-registration.
        when(registryClient.listWorkers())
                .thenReturn(List.of(worker("media", "ONLINE", "resizeImage")));
        service.refresh();

        assertThat(routingTable.resolveTopic("resizeImage", "t1")).contains("fuseflow-pool.media");
    }

    @Test
    void gainedCapabilityReDrivesRunnablePendingOnlyOnce() {
        // First refresh: a pool for resizeImage appears → the sweep re-drives the RUNNING
        // execution's runnable-PENDING activity through the scheduler.
        UUID executionId = UUID.randomUUID();
        when(registryClient.listWorkers())
                .thenReturn(List.of(worker("media", "ONLINE", "resizeImage")));
        when(activityRepository.findExecutionsWithRunnablePending()).thenReturn(List.of(executionId));
        when(executionRepository.findById(executionId)).thenReturn(Optional.of(
                new WorkflowExecution(executionId, UUID.randomUUID(), "wf", 1, "{\"k\":1}", null,
                        WorkflowStatus.RUNNING, 0, Instant.now(), Instant.now(), Instant.now(), null)));
        when(activityRepository.findRunnableTaskIds(executionId)).thenReturn(List.of("a"));
        ActivityExecution pending = new ActivityExecution(executionId, "a", "resizeImage", ActivityStatus.PENDING,
                0, List.of(), 1, null, null, 1, Instant.now(), Instant.now());
        when(activityRepository.findById(executionId, "a")).thenReturn(Optional.of(pending));

        service.refresh();
        verify(scheduler).schedule(eq(executionId), eq(List.of(pending)), eq("{\"k\":1}"));

        // A second refresh with no NEW capability must NOT re-sweep (a quiet fleet never pays
        // for the scan) — and must not double-schedule.
        service.refresh();
        verify(scheduler, times(1)).schedule(any(), any(), any());
    }

    @Test
    void refreshDoesNotSweepWhenRoutableSetUnchanged() {
        when(registryClient.listWorkers())
                .thenReturn(List.of(worker("media", "ONLINE", "resizeImage")));
        service.refresh();

        verify(activityRepository, times(1)).findExecutionsWithRunnablePending();
        verify(scheduler, never()).schedule(any(), any(), any());
    }

    @Test
    void onlineTransitionTriggersSweepForAlreadyKnownActivity() {
        // Regression: the activity is ALREADY in the table via an OFFLINE pool, so the raw
        // capability set never grows — the sweep must gate on the ROUTABLE (≥1 ONLINE) set, or
        // a pool coming online would never re-drive the PENDING tasks waiting for it.
        UUID executionId = UUID.randomUUID();
        when(activityRepository.findExecutionsWithRunnablePending()).thenReturn(List.of(executionId));
        when(executionRepository.findById(executionId)).thenReturn(Optional.of(
                new WorkflowExecution(executionId, UUID.randomUUID(), "wf", 1, "{\"k\":1}", null,
                        WorkflowStatus.RUNNING, 0, Instant.now(), Instant.now(), Instant.now(), null)));
        when(activityRepository.findRunnableTaskIds(executionId)).thenReturn(List.of("a"));
        ActivityExecution pending = new ActivityExecution(executionId, "a", "resizeImage", ActivityStatus.PENDING,
                0, List.of(), 1, null, null, 1, Instant.now(), Instant.now());
        when(activityRepository.findById(executionId, "a")).thenReturn(Optional.of(pending));

        // First refresh: pool OFFLINE → routable set empty → nothing to re-drive.
        when(registryClient.listWorkers())
                .thenReturn(List.of(worker("media", "OFFLINE", "resizeImage")));
        service.refresh();
        verify(scheduler, never()).schedule(any(), any(), any());

        // The same pool comes ONLINE → routable set grows → the waiting task is re-driven.
        when(registryClient.listWorkers())
                .thenReturn(List.of(worker("media", "ONLINE", "resizeImage")));
        service.refresh();
        verify(scheduler).schedule(eq(executionId), eq(List.of(pending)), eq("{\"k\":1}"));
    }
}
