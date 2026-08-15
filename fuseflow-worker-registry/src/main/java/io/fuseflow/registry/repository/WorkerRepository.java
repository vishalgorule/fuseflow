package io.fuseflow.registry.repository;

import io.fuseflow.registry.model.Worker;
import io.fuseflow.registry.model.WorkerActivity;
import io.fuseflow.registry.model.WorkerStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC access to the {@code registry} schema ({@code worker}, {@code worker_activity},
 * {@code worker_heartbeat}).
 *
 * <p>State transitions follow the project convention: a {@code version} column with guarded
 * conditional updates. Heartbeats are the one deliberate exception — they are unconditional
 * "touch" writes (a lost heartbeat race is harmless; the next heartbeat wins), while the
 * offline detector guards its downgrade on both status and the observed heartbeat timestamp
 * so a heartbeat that landed mid-scan is never downgraded away.
 */
@Repository
public class WorkerRepository {

    private static final String WORKER_TABLE = "registry.worker";
    private static final String ACTIVITY_TABLE = "registry.worker_activity";
    private static final String HEARTBEAT_TABLE = "registry.worker_heartbeat";

    private static final String WORKER_COLUMNS =
            "id, host, status, last_heartbeat_at, version, created_at, updated_at, pool_name, concurrency";

    private final JdbcClient jdbc;

    public WorkerRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insertWorker(Worker worker) {
        jdbc.sql("""
                        INSERT INTO %s (%s)
                        VALUES (:id, :host, :status, :lastHeartbeatAt, :version, :createdAt, :updatedAt,
                                :poolName, :concurrency)
                        """.formatted(WORKER_TABLE, WORKER_COLUMNS))
                .param("id", worker.id())
                .param("host", worker.host())
                .param("status", worker.status().name())
                .param("lastHeartbeatAt", Timestamp.from(worker.lastHeartbeatAt()))
                .param("version", worker.version())
                .param("createdAt", Timestamp.from(worker.createdAt()))
                .param("updatedAt", Timestamp.from(worker.updatedAt()))
                .param("poolName", worker.poolName())
                .param("concurrency", worker.concurrency(), Types.INTEGER)
                .update();
    }

    /**
     * Re-registration: the worker is alive again, so refresh host, revive to ONLINE and touch
     * the heartbeat. Optimistic update (returns false on version conflict).
     */
    public boolean updateOnRegister(UUID id, String host, String poolName,
                                    Integer concurrency, long expectedVersion) {
        // Pool identity refreshes when the worker declares it; otherwise it is preserved
        // (COALESCE) so legacy clients never silently reset a pool to the default.
        return jdbc.sql("""
                        UPDATE %s
                        SET host = :host, status = 'ONLINE',
                            pool_name = COALESCE(:poolName, pool_name),
                            concurrency = COALESCE(:concurrency, concurrency),
                            last_heartbeat_at = :now, version = version + 1, updated_at = :now
                        WHERE id = :id AND version = :expectedVersion
                        """.formatted(WORKER_TABLE))
                .param("id", id)
                .param("host", host)
                .param("poolName", poolName, Types.VARCHAR)
                .param("concurrency", concurrency, Types.INTEGER)
                .param("now", Timestamp.from(Instant.now()))
                .param("expectedVersion", expectedVersion)
                .update() == 1;
    }

    /**
     * Heartbeat touch: unconditional by design (see class javadoc). Returns the number of rows
     * updated (0 → worker does not exist).
     */
    public int touchHeartbeat(UUID id) {
        return jdbc.sql("""
                        UPDATE %s
                        SET status = 'ONLINE',
                            last_heartbeat_at = :now, version = version + 1, updated_at = :now
                        WHERE id = :id
                        """.formatted(WORKER_TABLE))
                .param("id", id)
                .param("now", Timestamp.from(Instant.now()))
                .update();
    }

    /** Appends a heartbeat to the append-only log (purged by the cleanup job). */
    public void appendHeartbeat(UUID id) {
        jdbc.sql("""
                        INSERT INTO %s (worker_id, received_at)
                        VALUES (:workerId, :receivedAt)
                        """.formatted(HEARTBEAT_TABLE))
                .param("workerId", id)
                .param("receivedAt", Timestamp.from(Instant.now()))
                .update();
    }

    /** Replaces the advertised activities of a worker (create and re-registration). */
    public void replaceActivities(UUID workerId, List<String> activities) {
        jdbc.sql("DELETE FROM " + ACTIVITY_TABLE + " WHERE worker_id = :workerId")
                .param("workerId", workerId)
                .update();
        for (String activity : activities) {
            jdbc.sql("INSERT INTO " + ACTIVITY_TABLE + " (worker_id, activity_name) VALUES (:workerId, :activity)")
                    .param("workerId", workerId)
                    .param("activity", activity)
                    .update();
        }
    }

    /** Deregisters a worker; cascades to its activities and heartbeats. False if unknown. */
    public boolean deleteWorker(UUID id) {
        return jdbc.sql("DELETE FROM " + WORKER_TABLE + " WHERE id = :id")
                .param("id", id)
                .update() == 1;
    }

    public Optional<Worker> findById(UUID id) {
        return jdbc.sql("SELECT " + WORKER_COLUMNS + " FROM " + WORKER_TABLE + " WHERE id = :id")
                .param("id", id)
                .query(this::mapWorker)
                .optional();
    }

    public List<Worker> findAll() {
        return jdbc.sql("SELECT " + WORKER_COLUMNS + " FROM " + WORKER_TABLE + " ORDER BY created_at DESC, id")
                .query(this::mapWorker)
                .list();
    }

    /** Workers whose liveness needs re-evaluation by the offline detector. */
    public List<Worker> findByStatusIn(List<WorkerStatus> statuses) {
        return jdbc.sql("SELECT " + WORKER_COLUMNS + " FROM " + WORKER_TABLE
                        + " WHERE status IN (:statuses) ORDER BY id")
                .param("statuses", statuses.stream().map(Enum::name).toList())
                .query(this::mapWorker)
                .list();
    }

    /** Capability lookup: workers advertising the given activity name (any status, FR-12). */
    public List<Worker> findByActivity(String activityName) {
        return jdbc.sql("""
                        SELECT w.%s FROM %s w
                        JOIN %s wa ON wa.worker_id = w.id
                        WHERE wa.activity_name = :activityName
                        ORDER BY w.id
                        """.formatted(WORKER_COLUMNS, WORKER_TABLE, ACTIVITY_TABLE))
                .param("activityName", activityName)
                .query(this::mapWorker)
                .list();
    }

    public List<WorkerActivity> findActivities(UUID workerId) {
        return jdbc.sql("SELECT worker_id, activity_name FROM " + ACTIVITY_TABLE
                        + " WHERE worker_id = :workerId ORDER BY activity_name")
                .param("workerId", workerId)
                .query(this::mapActivity)
                .list();
    }

    /** Batch loads activities for many workers in a single query (avoids N+1 on list). */
    public List<WorkerActivity> findActivitiesForWorkers(List<UUID> workerIds) {
        if (workerIds.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("SELECT worker_id, activity_name FROM " + ACTIVITY_TABLE
                        + " WHERE worker_id IN (:workerIds) ORDER BY worker_id, activity_name")
                .param("workerIds", workerIds)
                .query(this::mapActivity)
                .list();
    }

    /**
     * Offline-detector downgrade (ONLINE → DEGRADED / DEGRADED → OFFLINE). Guarded on the
     * observed status and heartbeat timestamp: if a heartbeat landed since the scan read the
     * row, the update no-ops and the worker stays (or becomes) ONLINE.
     */
    public boolean downgrade(UUID id, WorkerStatus expectedCurrent, WorkerStatus target, Instant observedLastHeartbeat) {
        return jdbc.sql("""
                        UPDATE %s
                        SET status = :target, version = version + 1, updated_at = :now
                        WHERE id = :id AND status = :expectedCurrent AND last_heartbeat_at = :observedHeartbeat
                        """.formatted(WORKER_TABLE))
                .param("id", id)
                .param("target", target.name())
                .param("expectedCurrent", expectedCurrent.name())
                .param("observedHeartbeat", Timestamp.from(observedLastHeartbeat))
                .param("now", Timestamp.from(Instant.now()))
                .update() == 1;
    }

    /** Purges heartbeat log rows older than the retention window. */
    public int deleteHeartbeatsBefore(Instant cutoff) {
        return jdbc.sql("DELETE FROM " + HEARTBEAT_TABLE + " WHERE received_at < :cutoff")
                .param("cutoff", Timestamp.from(cutoff))
                .update();
    }

    /** Removes workers that have been OFFLINE since before the removal grace period (FR-12). */
    public int deleteOfflineWorkersBefore(Instant cutoff) {
        return jdbc.sql("DELETE FROM " + WORKER_TABLE + " WHERE status = 'OFFLINE' AND updated_at < :cutoff")
                .param("cutoff", Timestamp.from(cutoff))
                .update();
    }

    private Worker mapWorker(ResultSet rs, int rowNum) throws SQLException {
        return new Worker(
                rs.getObject("id", UUID.class),
                rs.getString("host"),
                WorkerStatus.valueOf(rs.getString("status")),
                rs.getTimestamp("last_heartbeat_at").toInstant(),
                rs.getLong("version"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant(),
                rs.getString("pool_name"),
                rs.getObject("concurrency", Integer.class));
    }

    private WorkerActivity mapActivity(ResultSet rs, int rowNum) throws SQLException {
        return new WorkerActivity(rs.getObject("worker_id", UUID.class), rs.getString("activity_name"));
    }
}
