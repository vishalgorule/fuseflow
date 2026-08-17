package io.fuseflow.sdk.runtime;

import io.fuseflow.common.messaging.ActivityResultMessage;
import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.sdk.client.RegistryClient;
import io.fuseflow.sdk.config.WorkerProperties;
import io.fuseflow.sdk.core.ActivityContext;
import io.fuseflow.sdk.pub.ActivityResultPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.beans.factory.DisposableBean;
import tools.jackson.databind.ObjectMapper;

import java.net.InetAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The SDK worker runtime (Phase 4, FR-4/FR-5): on startup it registers with the registry
 * (with retries), then heartbeats on a configurable interval (re-registering if the registry
 * revives or the worker was removed); executes dispatched activities and publishes STARTED /
 * COMPLETED / FAILED results; and deregisters on shutdown.
 *
 * <p>Registration is best-effort and decoupled from execution: even with the registry down, a
 * worker can still execute activities over Kafka (the registry is only the discovery/health
 * view). At-least-once semantics hold — a redelivered task re-executes (activities are assumed
 * idempotent) and the engine dedupes results by {@code (executionId, taskId, attempt)}.
 */
public class FuseFlowWorker implements ApplicationRunner, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(FuseFlowWorker.class);

    private final UUID id;
    private final String host;
    private final Duration heartbeatInterval;
    private final String poolName;
    private final Integer concurrency;
    private final int registerRetries;
    private final Duration registerRetryDelay;
    private final ActivityRegistry activityRegistry;
    private final RegistryClient registryClient;
    private final ActivityResultPublisher resultPublisher;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService heartbeatExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "fuseflow-worker-heartbeat");
                thread.setDaemon(true);
                return thread;
            });

    public FuseFlowWorker(WorkerProperties properties,
                          ActivityRegistry activityRegistry,
                          RegistryClient registryClient,
                          ActivityResultPublisher resultPublisher,
                          ObjectMapper objectMapper) {
        this.id = properties.id() != null ? properties.id() : UUID.randomUUID();
        this.host = blankToDefault(properties.host(), defaultHost());
        this.heartbeatInterval = properties.heartbeatInterval() != null
                ? properties.heartbeatInterval() : Duration.ofSeconds(5);
        // Phase 5: the pool is a single knob — it drives the registered pool name, the
        // dispatch queue (fuseflow-pool.<pool>) and the consumer group, so the three can
        // never diverge.
        this.poolName = blankToDefault(properties.pool(), "default");
        this.concurrency = properties.concurrency();
        this.registerRetries = properties.registerRetries() != null ? properties.registerRetries() : 10;
        this.registerRetryDelay = properties.registerRetryDelay() != null
                ? properties.registerRetryDelay() : Duration.ofSeconds(2);
        this.activityRegistry = activityRegistry;
        this.registryClient = registryClient;
        this.resultPublisher = resultPublisher;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(ApplicationArguments args) {
        registerWithRetry();
        long intervalMillis = Math.max(1_000, heartbeatInterval.toMillis());
        heartbeatExecutor.scheduleWithFixedDelay(this::heartbeat, intervalMillis, intervalMillis,
                TimeUnit.MILLISECONDS);
    }

    /** Executes a dispatched activity and publishes STARTED + terminal results. */
    public void execute(ActivityTask task) {
        ActivityContext context = new ActivityContext(task.executionId(), task.taskId(),
                task.activityName(), task.attempt(), task.input());
        resultPublisher.publish(ActivityResultMessage.started(task));
        try {
            Object output = activityRegistry.execute(task.activityName(), context);
            resultPublisher.publish(ActivityResultMessage.completed(task, toJson(output)));
        } catch (Exception ex) {
            log.error("Activity {} of execution {} failed: {}",
                    task.activityName(), task.executionId(), ex.getMessage(), ex);
            // Phase 7: send the exception class name so the engine can classify the failure as
            // non-retryable per the retry policy's nonRetryableExceptions.
            resultPublisher.publish(ActivityResultMessage.failed(task, ex.getClass().getName(), ex.getMessage()));
        }
    }

    @Override
    public void destroy() {
        heartbeatExecutor.shutdownNow();
        try {
            registryClient.deregister(id);
            log.info("Worker {} deregistered", id);
        } catch (Exception ex) {
            log.warn("Failed to deregister worker {}: {}", id, ex.getMessage());
        }
    }

    // ---------------------------------------------------------------- internals

    private void registerWithRetry() {
        for (int attempt = 1; attempt <= Math.max(1, registerRetries); attempt++) {
            try {
                registryClient.register(id, host, activityRegistry.names(), poolName, concurrency);
                log.info("Worker {} registered (host={}, pool={}) with activities {}",
                        id, host, poolName, activityRegistry.names());
                return;
            } catch (Exception ex) {
                log.warn("Worker registration attempt {}/{} failed: {}", attempt, registerRetries,
                        ex.getMessage());
                if (attempt < registerRetries) {
                    sleep(registerRetryDelay);
                }
            }
        }
        log.error("Worker {} could not register after {} attempts — dispatch over Kafka still " +
                "works, but the worker is invisible to the registry until it recovers", id, registerRetries);
    }

    private void heartbeat() {
        try {
            registryClient.heartbeat(id);
        } catch (Exception ex) {
            // Registry unreachable, or the worker was removed (404): re-register to revive.
            log.warn("Heartbeat failed for worker {}: {}", id, ex.getMessage());
            try {
                registryClient.register(id, host, activityRegistry.names(), poolName, concurrency);
            } catch (Exception ignored) {
                // Will retry on the next heartbeat.
            }
        }
    }

    private String toJson(Object output) {
        if (output == null) {
            return null;
        }
        if (output instanceof String string) {
            return string;
        }
        try {
            return objectMapper.writeValueAsString(output);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize activity output", ex);
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private static String blankToDefault(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String defaultHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception ex) {
            return "unknown-host";
        }
    }
}
