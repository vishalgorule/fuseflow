package io.fuseflow.engine.registry;

import io.fuseflow.common.dto.WorkerResponse;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import io.fuseflow.engine.service.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.Ordered;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Owns the pool routing lifecycle (Phase 5): seeds the {@link PoolRoutingTable} from the
 * registry at boot (before {@code ExecutionRecovery}, so recovery dispatches never hit an empty
 * table) and re-seeds on every {@code worker-events} message (pool membership or liveness
 * changed). Pool topics are provisioned after each seed. Failures are logged and retried on the
 * next event — the registry is never on the dispatch hot path.
 *
 * <p>Post-Phase 7 hardening: when a refresh <em>gains</em> a capability (a pool for an activity
 * that previously had none), the sweep re-drives runnable-PENDING activities across RUNNING
 * executions — tasks that were left PENDING because no pool could route them at schedule time
 * (see {@link Scheduler}) finally get scheduled. Gated on growth so a quiet fleet never pays
 * for the scan; the re-drive is idempotent (every transition is version-guarded).
 */
@Component
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "kafka", matchIfMissing = true)
public class PoolRoutingService implements ApplicationRunner, Ordered {

    private static final Logger log = LoggerFactory.getLogger(PoolRoutingService.class);

    private final EngineRegistryClient registryClient;
    private final PoolRoutingTable routingTable;
    private final PoolTopicProvisioner topicProvisioner;
    private final ActivityExecutionRepository activityRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final Scheduler scheduler;

    /** Capability set from the previous refresh — growth detection for the rejoin sweep. */
    private volatile Set<String> previousActivities = Set.of();

    public PoolRoutingService(EngineRegistryClient registryClient,
                              PoolRoutingTable routingTable,
                              PoolTopicProvisioner topicProvisioner,
                              ActivityExecutionRepository activityRepository,
                              WorkflowExecutionRepository executionRepository,
                              Scheduler scheduler) {
        this.registryClient = registryClient;
        this.routingTable = routingTable;
        this.topicProvisioner = topicProvisioner;
        this.activityRepository = activityRepository;
        this.executionRepository = executionRepository;
        this.scheduler = scheduler;
    }

    @Override
    public void run(ApplicationArguments args) {
        refresh();
    }

    /**
     * Self-healing cadence (Phase 5 HA): re-seeds the routing table from the registry even
     * with no worker-events, so a missed/lost event (or a heartbeat revival that raced the
     * published {@code worker_online}) can never leave dispatch pointing at a stale snapshot.
     * Read-only when nothing changed; safe to run concurrently with event-driven refreshes.
     */
    @Scheduled(fixedDelayString = "${fuseflow.engine.routing.refresh-interval:30s}")
    public void refreshPeriodically() {
        refresh();
    }

    /** Re-seeds the routing table from the registry and provisions any new pool topics. */
    public synchronized void refresh() {
        try {
            List<WorkerResponse> workers = registryClient.listWorkers();
            routingTable.seed(workers);
            for (String pool : routingTable.poolNames()) {
                topicProvisioner.ensure(pool, routingTable.poolConcurrency(pool));
            }
            // Post-Phase 7 hardening: only when the ROUTABLE set GREW (a pool for an activity
            // came ONLINE — including an activity already in the table via an OFFLINE pool) do
            // runnable-PENDING activities need re-driving — a quiet fleet never pays for the scan.
            Set<String> current = routingTable.routableActivities();
            if (!previousActivities.containsAll(current)) {
                reDriveRunnablePending();
            }
            previousActivities = current;
            log.info("Pool routing refreshed: {} pool(s), {} activity(ies)",
                    routingTable.poolNames().size(), routingTable.size());
        } catch (Exception ex) {
            // Registry down at boot, or a transient failure — the table keeps its last good
            // snapshot and the next worker-events message retries.
            log.warn("Pool routing refresh failed (keeping previous snapshot): {}", ex.getMessage());
        }
    }

    /**
     * Re-drives activities that were left PENDING because no pool could route them at schedule
     * time. {@link Scheduler#schedule} re-checks routing per task, so only newly-routable ones
     * are actually scheduled; everything else stays PENDING for the next growth event.
     */
    private void reDriveRunnablePending() {
        List<UUID> executionIds = activityRepository.findExecutionsWithRunnablePending();
        if (executionIds.isEmpty()) {
            return;
        }
        log.info("Routing gained capabilities — re-driving runnable PENDING activities of {} execution(s)",
                executionIds.size());
        for (UUID executionId : executionIds) {
            WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
            if (execution == null || execution.status() != WorkflowStatus.RUNNING) {
                continue;
            }
            List<ActivityExecution> runnable = activityRepository.findRunnableTaskIds(executionId).stream()
                    .flatMap(taskId -> activityRepository.findById(executionId, taskId).stream())
                    .toList();
            if (!runnable.isEmpty()) {
                scheduler.schedule(executionId, runnable, execution.input());
            }
        }
    }

    /** Runs before every other {@link ApplicationRunner} so recovery dispatches see a seeded table. */
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
