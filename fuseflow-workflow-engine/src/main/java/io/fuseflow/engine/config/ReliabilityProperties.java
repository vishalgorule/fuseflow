package io.fuseflow.engine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/**
 * Phase 7 reliability configuration ({@code fuseflow.engine.*}). These are the engine-side
 * defaults the retry manager falls back to when neither the task nor the workflow definition
 * specifies a knob — the resolution order is task policy → workflow policy → here.
 *
 * <ul>
 *   <li>{@code fuseflow.engine.retry.default-max-attempts} — total attempts incl. the first (3)</li>
 *   <li>{@code fuseflow.engine.retry.default-fixed-delay} — base delay between attempts (5s)</li>
 *   <li>{@code fuseflow.engine.retry.default-exponential-backoff} — delay grows by the
 *       multiplier per attempt (true)</li>
 *   <li>{@code fuseflow.engine.retry.default-backoff-multiplier} — backoff growth (2.0)</li>
 *   <li>{@code fuseflow.engine.retry.default-non-retryable-exceptions} — never retry these
 *       exception class names (exact or trailing {@code *})</li>
 *   <li>{@code fuseflow.engine.timeout.start} — SCHEDULED → STARTED window (60s); a task that
 *       never starts within it is treated as a failed attempt</li>
 *   <li>{@code fuseflow.engine.timeout.execution} — STARTED → result window (300s); covers
 *       hung/dead workers (the registry's heartbeat detection drives routing away from dead
 *       pools, this drives the retry)</li>
 *   <li>{@code fuseflow.engine.poll-interval} — cadence of the retry + timeout poller (5s)</li>
 *   <li>{@code fuseflow.engine.poll-batch-size} — max due retries claimed per poll cycle, so a
 *       burst (e.g. a correlated outage) drains across cycles instead of one giant in-memory
 *       batch (500)</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "fuseflow.engine")
public class ReliabilityProperties {

    private Retry retry = new Retry();
    private Timeout timeout = new Timeout();
    private Duration pollInterval = Duration.ofSeconds(5);
    private int pollBatchSize = 500;

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    public Timeout getTimeout() {
        return timeout;
    }

    public void setTimeout(Timeout timeout) {
        this.timeout = timeout;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public int getPollBatchSize() {
        return pollBatchSize;
    }

    public void setPollBatchSize(int pollBatchSize) {
        this.pollBatchSize = pollBatchSize;
    }

    public static class Retry {
        private int defaultMaxAttempts = 3;
        private Duration defaultFixedDelay = Duration.ofSeconds(5);
        private boolean defaultExponentialBackoff = true;
        private double defaultBackoffMultiplier = 2.0;
        private List<String> defaultNonRetryableExceptions = List.of();

        public int getDefaultMaxAttempts() {
            return defaultMaxAttempts;
        }

        public void setDefaultMaxAttempts(int defaultMaxAttempts) {
            this.defaultMaxAttempts = defaultMaxAttempts;
        }

        public Duration getDefaultFixedDelay() {
            return defaultFixedDelay;
        }

        public void setDefaultFixedDelay(Duration defaultFixedDelay) {
            this.defaultFixedDelay = defaultFixedDelay;
        }

        public boolean isDefaultExponentialBackoff() {
            return defaultExponentialBackoff;
        }

        public void setDefaultExponentialBackoff(boolean defaultExponentialBackoff) {
            this.defaultExponentialBackoff = defaultExponentialBackoff;
        }

        public double getDefaultBackoffMultiplier() {
            return defaultBackoffMultiplier;
        }

        public void setDefaultBackoffMultiplier(double defaultBackoffMultiplier) {
            this.defaultBackoffMultiplier = defaultBackoffMultiplier;
        }

        public List<String> getDefaultNonRetryableExceptions() {
            return defaultNonRetryableExceptions;
        }

        public void setDefaultNonRetryableExceptions(List<String> defaultNonRetryableExceptions) {
            this.defaultNonRetryableExceptions = defaultNonRetryableExceptions;
        }
    }

    public static class Timeout {
        private Duration start = Duration.ofSeconds(60);
        private Duration execution = Duration.ofSeconds(300);

        public Duration getStart() {
            return start;
        }

        public void setStart(Duration start) {
            this.start = start;
        }

        public Duration getExecution() {
            return execution;
        }

        public void setExecution(Duration execution) {
            this.execution = execution;
        }
    }
}
