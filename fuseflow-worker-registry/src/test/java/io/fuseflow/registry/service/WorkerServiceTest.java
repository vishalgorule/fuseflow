package io.fuseflow.registry.service;

import io.fuseflow.common.exception.ApiException;
import io.fuseflow.registry.messaging.WorkerEventPublisher;
import io.fuseflow.registry.model.Worker;
import io.fuseflow.registry.model.WorkerStatus;
import io.fuseflow.registry.repository.WorkerRepository;
import io.fuseflow.registry.validation.WorkerValidator;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the worker lifecycle service (Phase 3 + Phase 5 HA fix): the heartbeat
 * path. A heartbeat that revives a DEGRADED/OFFLINE worker must publish {@code worker_online}
 * — observers (the engine's pool routing table) only learn liveness via worker-events, so
 * without the revival event a transient heartbeat gap leaves the pool unroutable until the
 * worker re-registers.
 */
class WorkerServiceTest {

    private final WorkerRepository repository = mock(WorkerRepository.class);
    private final WorkerValidator validator = mock(WorkerValidator.class);
    private final WorkerEventPublisher publisher = mock(WorkerEventPublisher.class);
    private final WorkerService service = new WorkerService(repository, validator, publisher);

    private static Worker worker(UUID id, WorkerStatus status) {
        Instant now = Instant.now();
        return new Worker(id, "host", status, now, 0, now, now, "media", 4);
    }

    @Test
    void heartbeatOnOnlineWorkerIsSilent() {
        UUID id = UUID.randomUUID();
        when(repository.findById(id)).thenReturn(Optional.of(worker(id, WorkerStatus.ONLINE)));

        service.heartbeat(id);

        verify(repository).touchHeartbeat(id);
        verify(repository).appendHeartbeat(id);
        verify(publisher, never()).publish(any(), any(), any());
    }

    @Test
    void heartbeatRevivingDegradedWorkerPublishesWorkerOnline() {
        UUID id = UUID.randomUUID();
        when(repository.findById(id)).thenReturn(Optional.of(worker(id, WorkerStatus.DEGRADED)));

        service.heartbeat(id);

        verify(repository).touchHeartbeat(id);
        verify(repository).appendHeartbeat(id);
        verify(publisher).publish(eq(id), eq("worker_online"), any());
    }

    @Test
    void heartbeatRevivingOfflineWorkerPublishesWorkerOnline() {
        UUID id = UUID.randomUUID();
        when(repository.findById(id)).thenReturn(Optional.of(worker(id, WorkerStatus.OFFLINE)));

        service.heartbeat(id);

        verify(repository).touchHeartbeat(id);
        verify(repository).appendHeartbeat(id);
        verify(publisher).publish(eq(id), eq("worker_online"), any());
    }

    @Test
    void heartbeatOnUnknownWorkerThrowsNotFound() {
        UUID id = UUID.randomUUID();
        when(repository.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.heartbeat(id))
                .isInstanceOf(ApiException.class)
                .satisfies(ex -> {
                    ApiException api = (ApiException) ex;
                    assertThat(api.getCode()).isEqualTo("worker_not_found");
                });
    }
}
