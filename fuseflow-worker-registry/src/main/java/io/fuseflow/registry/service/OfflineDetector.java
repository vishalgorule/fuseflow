package io.fuseflow.registry.service;

import io.fuseflow.registry.messaging.WorkerEventPublisher;
import io.fuseflow.registry.model.Worker;
import io.fuseflow.registry.model.WorkerStatus;
import io.fuseflow.registry.repository.WorkerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Offline detection + cleanup (Phase 3, FR-12): a scheduled job that
 * <ul>
 *   <li>marks workers {@code DEGRADED} then {@code OFFLINE} once they have missed heartbeats
 *       beyond the configured windows ({@code degraded-after} / {@code timeout}),</li>
 *   <li>never revives — only {@code POST /workers/{id}/heartbeat} brings a worker back,</li>
 *   <li>purges heartbeat log rows older than the retention window, and</li>
 *   <li>removes workers that have been {@code OFFLINE} beyond the removal grace period.</li>
 * </ul>
 *
 * <p>Downgrades are guarded on the observed status + heartbeat timestamp (see
 * {@link WorkerRepository#downgrade}): a heartbeat landing mid-scan is never downgraded away.
 * {@code detect} and {@code cleanup} are package-private with an explicit {@code now} so unit
 * tests can drive them deterministically.
 */
@Component
public class OfflineDetector {

    private static final Logger log = LoggerFactory.getLogger(OfflineDetector.class);

    private final WorkerRepository workerRepository;
    private final WorkerEventPublisher workerEventPublisher;
    private final Duration degradedAfter;
    private final Duration offlineAfter;
    private final Duration heartbeatRetention;
    private final Duration offlineRemovalAfter;

    public OfflineDetector(WorkerRepository workerRepository,
                           WorkerEventPublisher workerEventPublisher,
                           @Value("${fuseflow.registry.heartbeat.degraded-after:15s}") Duration degradedAfter,
                           @Value("${fuseflow.registry.heartbeat.timeout:30s}") Duration offlineAfter,
                           @Value("${fuseflow.registry.heartbeat.retention:24h}") Duration heartbeatRetention,
                           @Value("${fuseflow.registry.heartbeat.offline-removal-after:7d}") Duration offlineRemovalAfter) {
        this.workerRepository = workerRepository;
        this.workerEventPublisher = workerEventPublisher;
        this.degradedAfter = degradedAfter;
        this.offlineAfter = offlineAfter;
        this.heartbeatRetention = heartbeatRetention;
        this.offlineRemovalAfter = offlineRemovalAfter;
    }

    @Scheduled(fixedDelayString = "${fuseflow.registry.detection.interval:5s}")
    public void run() {
        Instant now = Instant.now();
        detect(now);
        cleanup(now);
    }

    /** Downgrades workers whose heartbeat is stale. */
    void detect(Instant now) {
        for (Worker worker : workerRepository.findByStatusIn(List.of(WorkerStatus.ONLINE, WorkerStatus.DEGRADED))) {
            WorkerStatus target = evaluate(worker.lastHeartbeatAt(), now, degradedAfter, offlineAfter);
            if (target != worker.status()
                    && workerRepository.downgrade(worker.id(), worker.status(), target, worker.lastHeartbeatAt())) {
                log.info("Worker {} {} -> {}", worker.id(), worker.status(), target);
                if (target == WorkerStatus.OFFLINE) {
                    workerEventPublisher.publish(worker.id(), "worker_offline",
                            Map.of("lastHeartbeatAt", worker.lastHeartbeatAt().toString()));
                }
            }
        }
    }

    /** Purges the heartbeat log and removes long-offline workers. */
    void cleanup(Instant now) {
        int purgedHeartbeats = workerRepository.deleteHeartbeatsBefore(now.minus(heartbeatRetention));
        int removedWorkers = workerRepository.deleteOfflineWorkersBefore(now.minus(offlineRemovalAfter));
        if (purgedHeartbeats > 0 || removedWorkers > 0) {
            log.info("Cleanup: purged {} stale heartbeat(s), removed {} long-offline worker(s)",
                    purgedHeartbeats, removedWorkers);
        }
    }

    /**
     * Derives the target liveness status from heartbeat freshness: older than the timeout →
     * OFFLINE; older than {@code degradedAfter} → DEGRADED; else ONLINE. {@code lastHeartbeatAt}
     * is never null in the DB ({@code NOT NULL} column, set on register and every heartbeat).
     */
    static WorkerStatus evaluate(Instant lastHeartbeatAt, Instant now, Duration degradedAfter, Duration offlineAfter) {
        Duration age = Duration.between(lastHeartbeatAt, now);
        if (age.compareTo(offlineAfter) >= 0) {
            return WorkerStatus.OFFLINE;
        }
        if (age.compareTo(degradedAfter) >= 0) {
            return WorkerStatus.DEGRADED;
        }
        return WorkerStatus.ONLINE;
    }
}
