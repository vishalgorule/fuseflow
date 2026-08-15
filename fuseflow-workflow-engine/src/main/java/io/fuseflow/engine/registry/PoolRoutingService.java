package io.fuseflow.engine.registry;

import io.fuseflow.common.dto.WorkerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Owns the pool routing lifecycle (Phase 5): seeds the {@link PoolRoutingTable} from the
 * registry at boot (before {@code ExecutionRecovery}, so recovery dispatches never hit an empty
 * table) and re-seeds on every {@code worker-events} message (pool membership or liveness
 * changed). Pool topics are provisioned after each seed. Failures are logged and retried on the
 * next event — the registry is never on the dispatch hot path.
 */
@Component
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "kafka", matchIfMissing = true)
public class PoolRoutingService implements ApplicationRunner, Ordered {

    private static final Logger log = LoggerFactory.getLogger(PoolRoutingService.class);

    private final EngineRegistryClient registryClient;
    private final PoolRoutingTable routingTable;
    private final PoolTopicProvisioner topicProvisioner;

    public PoolRoutingService(EngineRegistryClient registryClient,
                              PoolRoutingTable routingTable,
                              PoolTopicProvisioner topicProvisioner) {
        this.registryClient = registryClient;
        this.routingTable = routingTable;
        this.topicProvisioner = topicProvisioner;
    }

    @Override
    public void run(ApplicationArguments args) {
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
            log.info("Pool routing refreshed: {} pool(s), {} activity(ies)",
                    routingTable.poolNames().size(), routingTable.size());
        } catch (Exception ex) {
            // Registry down at boot, or a transient failure — the table keeps its last good
            // snapshot and the next worker-events message retries.
            log.warn("Pool routing refresh failed (keeping previous snapshot): {}", ex.getMessage());
        }
    }

    /** Runs before every other {@link ApplicationRunner} so recovery dispatches see a seeded table. */
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
