package io.fuseflow.engine.repository;

import io.fuseflow.engine.model.DagModel;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the dispatch outbox (post-Phase 7 hardening) against real PostgreSQL:
 * the outbox round-trip (insert → publish → mark) and — the high-risk part — the correlated
 * exclusions in {@link ActivityExecutionRepository#findStartTimeouts} and
 * {@link ActivityExecutionRepository#findStale} that keep outbox-waiting and freshly-published
 * tasks off the start-timeout/retry clocks.
 *
 * <p>{@code timeout.start=1h} so the engine's own {@code TimeoutManager} never fires on the
 * backdated rows these tests seed.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@TestPropertySource(properties = "fuseflow.engine.timeout.start=1h")
class DispatchOutboxRepositoryTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18.4");

    @Autowired
    JdbcClient jdbc;

    @Autowired
    DispatchOutboxRepository outboxRepository;

    @Autowired
    ActivityExecutionRepository activityRepository;

    @Autowired
    WorkflowExecutionRepository executionRepository;

    /** The tests share one container — keep each one hermetic (the engine tables only). */
    @BeforeEach
    void cleanEngineTables() {
        jdbc.sql("DELETE FROM engine.dispatch_outbox").update();
        jdbc.sql("DELETE FROM engine.activity_execution").update();
        jdbc.sql("DELETE FROM engine.workflow_execution").update();
    }

    /** Seeds a RUNNING execution with one SCHEDULED activity backdated 5 minutes (start-timeout eligible). */
    private UUID seedScheduledActivity() {
        UUID executionId = UUID.randomUUID();
        Instant now = Instant.now();
        executionRepository.insert(new WorkflowExecution(executionId, UUID.randomUUID(), "wf", 1,
                "{\"k\":1}", null, WorkflowStatus.RUNNING, 0, now, now, now, null), 1);
        activityRepository.insertAll(executionId, List.of(new DagModel.DagTask("a", "actA", 0, List.of())));
        jdbc.sql("""
                        UPDATE engine.activity_execution
                        SET status = 'SCHEDULED', updated_at = :old
                        WHERE workflow_execution_id = :id AND task_id = 'a'
                        """)
                .param("old", Timestamp.from(now.minusSeconds(300)))
                .param("id", executionId)
                .update();
        return executionId;
    }

    @Test
    void outboxRoundTripInsertFindPublishUnroutable() {
        UUID executionId = UUID.randomUUID();

        outboxRepository.insert(executionId, "a", "actA", "{\"k\":1}", 1);
        outboxRepository.insert(executionId, "a", "actA", "{\"k\":1}", 1); // idempotent per (exec, task, attempt)
        assertThat(outboxRepository.findPending(100)).hasSize(1);

        DispatchOutboxRepository.Entry entry = outboxRepository.findPending(100).get(0);
        assertThat(entry.workflowExecutionId()).isEqualTo(executionId);
        assertThat(entry.attempt()).isEqualTo(1);

        // First unroutable sighting records the reason; a second sighting is a no-op (event dedup).
        assertThat(outboxRepository.markUnroutable(entry.id(), "no pool")).isTrue();
        assertThat(outboxRepository.markUnroutable(entry.id(), "no pool")).isFalse();

        // Publishing removes it from the PENDING poll.
        assertThat(outboxRepository.markPublished(entry.id())).isTrue();
        assertThat(outboxRepository.findPending(100)).isEmpty();
    }

    @Test
    void startTimeoutFindsGenuinelyStuckScheduledRow() {
        UUID executionId = seedScheduledActivity();
        List<io.fuseflow.engine.model.ActivityExecution> timeouts =
                activityRepository.findStartTimeouts(Instant.now().minusSeconds(60));
        assertThat(timeouts).extracting(io.fuseflow.engine.model.ActivityExecution::taskId).contains("a");
    }

    @Test
    void startTimeoutExcludesRowsWaitingInOutbox() {
        UUID executionId = seedScheduledActivity();
        outboxRepository.insert(executionId, "a", "actA", "{\"k\":1}", 1);

        List<io.fuseflow.engine.model.ActivityExecution> timeouts =
                activityRepository.findStartTimeouts(Instant.now().minusSeconds(60));

        // Waiting for a pool is NOT a timeout — the outbox poller owns the dispatch.
        assertThat(timeouts).isEmpty();
    }

    @Test
    void startTimeoutExcludesRecentlyPublishedRowsButNotOldOnes() {
        UUID executionId = seedScheduledActivity();
        outboxRepository.insert(executionId, "a", "actA", "{\"k\":1}", 1);
        UUID outboxId = outboxRepository.findPending(100).get(0).id();
        outboxRepository.markPublished(outboxId);

        // Freshly published (published_at = now): the 60s window starts at publish — excluded.
        assertThat(activityRepository.findStartTimeouts(Instant.now().minusSeconds(60))).isEmpty();

        // Backdate the publish past the window: a published-but-never-started task is a real
        // start timeout again (the task waited in the outbox for a pool, then never started).
        jdbc.sql("UPDATE engine.dispatch_outbox SET published_at = :old WHERE id = :id")
                .param("old", Timestamp.from(Instant.now().minusSeconds(300)))
                .param("id", outboxId)
                .update();
        List<io.fuseflow.engine.model.ActivityExecution> timeouts =
                activityRepository.findStartTimeouts(Instant.now().minusSeconds(60));
        assertThat(timeouts).extracting(io.fuseflow.engine.model.ActivityExecution::taskId).contains("a");
    }

    @Test
    void findStaleSkipsPendingOutboxButReDispatchesPublishedOnes() {
        UUID executionId = seedScheduledActivity();
        outboxRepository.insert(executionId, "a", "actA", "{\"k\":1}", 1);

        // PENDING outbox row → the outbox poller owns the dispatch; recovery must not touch it.
        assertThat(activityRepository.findStale(executionId)).isEmpty();

        // PUBLISHED row (message lost to a broker outage) → recovery re-dispatches it.
        UUID outboxId = outboxRepository.findPending(100).get(0).id();
        outboxRepository.markPublished(outboxId);
        assertThat(activityRepository.findStale(executionId))
                .extracting(io.fuseflow.engine.model.ActivityExecution::taskId).contains("a");
    }

    @Test
    void findsExecutionsWithRunnablePending() {
        UUID executionId = UUID.randomUUID();
        Instant now = Instant.now();
        executionRepository.insert(new WorkflowExecution(executionId, UUID.randomUUID(), "wf", 1,
                "{\"k\":1}", null, WorkflowStatus.RUNNING, 0, now, now, now, null), 1);
        activityRepository.insertAll(executionId, List.of(new DagModel.DagTask("a", "actA", 0, List.of())));

        assertThat(activityRepository.findExecutionsWithRunnablePending()).contains(executionId);
    }
}
