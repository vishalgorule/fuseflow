package io.fuseflow.registry.service;

import io.fuseflow.registry.messaging.WorkerEventPublisher;
import io.fuseflow.registry.model.Worker;
import io.fuseflow.registry.model.WorkerStatus;
import io.fuseflow.registry.repository.WorkerRepository;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OfflineDetectorTest {

    private static final Duration DEGRADED_AFTER = Duration.ofSeconds(10);
    private static final Duration OFFLINE_AFTER = Duration.ofSeconds(30);
    private static final Duration RETENTION = Duration.ofHours(1);
    private static final Duration REMOVAL_AFTER = Duration.ofDays(1);

    private final WorkerRepository repository = mock(WorkerRepository.class);
    private final WorkerEventPublisher workerEventPublisher = mock(WorkerEventPublisher.class);
    private final OfflineDetector detector =
            new OfflineDetector(repository, workerEventPublisher, DEGRADED_AFTER, OFFLINE_AFTER, RETENTION, REMOVAL_AFTER);

    private static Worker worker(UUID id, WorkerStatus status, Instant lastHeartbeatAt) {
        Instant now = Instant.now();
        return new Worker(id, "host", status, lastHeartbeatAt, 0, now, now);
    }

    @Test
    void downgradesWorkerMissingAllHeartbeatsToOffline() {
        Instant now = Instant.now();
        Worker stale = worker(UUID.randomUUID(), WorkerStatus.ONLINE, now.minus(Duration.ofSeconds(40)));
        when(repository.findByStatusIn(any())).thenReturn(List.of(stale));
        when(repository.downgrade(stale.id(), WorkerStatus.ONLINE, WorkerStatus.OFFLINE, stale.lastHeartbeatAt()))
                .thenReturn(true);

        detector.detect(now);

        verify(repository).downgrade(stale.id(), WorkerStatus.ONLINE, WorkerStatus.OFFLINE, stale.lastHeartbeatAt());
        // The transition to OFFLINE publishes a worker_offline event (Phase 4).
        verify(workerEventPublisher).publish(eq(stale.id()), eq("worker_offline"), any());
    }

    @Test
    void downgradesWorkerMissingSomeHeartbeatsToDegraded() {
        Instant now = Instant.now();
        Worker stale = worker(UUID.randomUUID(), WorkerStatus.ONLINE, now.minus(Duration.ofSeconds(20)));
        when(repository.findByStatusIn(any())).thenReturn(List.of(stale));

        detector.detect(now);

        verify(repository).downgrade(stale.id(), WorkerStatus.ONLINE, WorkerStatus.DEGRADED, stale.lastHeartbeatAt());
    }

    @Test
    void leavesHealthyWorkersAlone() {
        Instant now = Instant.now();
        Worker healthy = worker(UUID.randomUUID(), WorkerStatus.ONLINE, now.minus(Duration.ofSeconds(5)));
        when(repository.findByStatusIn(any())).thenReturn(List.of(healthy));

        detector.detect(now);

        verify(repository, never()).downgrade(any(), any(), any(), any());
    }

    @Test
    void skipsAlreadyDegradedWorkersThatAreStillWithinTimeout() {
        Instant now = Instant.now();
        Worker degraded = worker(UUID.randomUUID(), WorkerStatus.DEGRADED, now.minus(Duration.ofSeconds(20)));
        when(repository.findByStatusIn(any())).thenReturn(List.of(degraded));

        detector.detect(now);

        verify(repository, never()).downgrade(any(), any(), any(), any());
    }

    @Test
    void survivesLostDowngradeRace() {
        // A heartbeat landed mid-scan, so the downgrade no-ops — nothing should blow up.
        Instant now = Instant.now();
        Worker stale = worker(UUID.randomUUID(), WorkerStatus.DEGRADED, now.minus(Duration.ofSeconds(40)));
        when(repository.findByStatusIn(any())).thenReturn(List.of(stale));
        when(repository.downgrade(stale.id(), WorkerStatus.DEGRADED, WorkerStatus.OFFLINE, stale.lastHeartbeatAt()))
                .thenReturn(false);

        detector.detect(now);

        verify(repository).downgrade(stale.id(), WorkerStatus.DEGRADED, WorkerStatus.OFFLINE, stale.lastHeartbeatAt());
    }

    @Test
    void cleanupPurgesHeartbeatsAndRemovesLongOfflineWorkers() {
        Instant now = Instant.now();

        detector.cleanup(now);

        verify(repository).deleteHeartbeatsBefore(now.minus(RETENTION));
        verify(repository).deleteOfflineWorkersBefore(now.minus(REMOVAL_AFTER));
    }

    @Test
    void evaluateMapsHeartbeatAgeToStatus() {
        Instant now = Instant.now();
        assertThat(OfflineDetector.evaluate(now.minus(Duration.ofSeconds(5)), now, DEGRADED_AFTER, OFFLINE_AFTER))
                .isEqualTo(WorkerStatus.ONLINE);
        assertThat(OfflineDetector.evaluate(now.minus(Duration.ofSeconds(20)), now, DEGRADED_AFTER, OFFLINE_AFTER))
                .isEqualTo(WorkerStatus.DEGRADED);
        assertThat(OfflineDetector.evaluate(now.minus(Duration.ofSeconds(40)), now, DEGRADED_AFTER, OFFLINE_AFTER))
                .isEqualTo(WorkerStatus.OFFLINE);
    }
}
