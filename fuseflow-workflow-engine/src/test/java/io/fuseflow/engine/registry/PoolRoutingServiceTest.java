package io.fuseflow.engine.registry;

import io.fuseflow.common.dto.WorkerResponse;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PoolRoutingService} (Phase 5 HA fix): the routing table re-seeds from
 * the registry on refresh — the periodic {@code refreshPeriodically} cadence and every
 * worker-events message both funnel here — so a worker that was marked OFFLINE and then revived
 * by a heartbeat becomes routable again on the next refresh even if the {@code worker_online}
 * event was missed.
 */
class PoolRoutingServiceTest {

    private final EngineRegistryClient registryClient = mock(EngineRegistryClient.class);
    private final PoolRoutingTable routingTable = new PoolRoutingTable("fuseflow-pool");
    private final PoolTopicProvisioner topicProvisioner = mock(PoolTopicProvisioner.class);
    private final PoolRoutingService service =
            new PoolRoutingService(registryClient, routingTable, topicProvisioner);

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
}
