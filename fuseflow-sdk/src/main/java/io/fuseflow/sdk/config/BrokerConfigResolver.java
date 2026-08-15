package io.fuseflow.sdk.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Resolves the broker address from the registry's discovery endpoint
 * ({@code GET /api/v1/config}) — the registry is the <b>only</b> endpoint a worker is
 * configured with, so the SDK can set up its transport without the user knowing any Kafka
 * internals.
 *
 * <p>There is <b>no fallback</b>: {@link #resolve} blocks until the registry answers with a
 * broker, retrying at a fixed interval (default 5s). A worker is not functional without the
 * registry — it must register and heartbeat there anyway — so discovery waits for it rather than
 * booting against a guessed broker (e.g. {@code localhost:9092} when the real one is
 * {@code kafka:9092}). Once the registry is reachable, the correct broker is used and the worker
 * proceeds to register.
 */
final class BrokerConfigResolver {

    private static final Logger log = LoggerFactory.getLogger(BrokerConfigResolver.class);

    private static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofSeconds(5);

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Duration retryInterval;

    BrokerConfigResolver() {
        this(DEFAULT_RETRY_INTERVAL);
    }

    /** Package-visible for tests (short interval). */
    BrokerConfigResolver(Duration retryInterval) {
        this.retryInterval = retryInterval;
    }

    /**
     * Blocks until the registry's config endpoint returns a broker. Retries at a fixed interval
     * on any failure (unreachable registry, error status, or a payload without
     * {@code bootstrapServers}).
     */
    String resolve(String registryBaseUrl) {
        URI uri = URI.create(stripTrailingSlash(registryBaseUrl) + "/api/v1/config");
        while (true) {
            try {
                HttpRequest request = HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofSeconds(2))
                        .GET()
                        .build();
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    Map<?, ?> body = objectMapper.readValue(response.body(), Map.class);
                    Object servers = body.get("bootstrapServers");
                    if (servers instanceof String value && !value.isBlank()) {
                        return value;
                    }
                    log.warn("Registry {} answered without bootstrapServers — retrying in {}",
                            registryBaseUrl, retryInterval);
                } else {
                    log.warn("Registry {} returned {} for broker discovery — retrying in {}",
                            registryBaseUrl, response.statusCode(), retryInterval);
                }
            } catch (Exception ex) {
                log.warn("Broker discovery from registry {} failed: {} — retrying in {}",
                        registryBaseUrl, ex.getMessage(), retryInterval);
            }
            sleep(retryInterval);
        }
    }

    private static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
