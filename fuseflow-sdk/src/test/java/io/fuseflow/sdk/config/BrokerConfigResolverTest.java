package io.fuseflow.sdk.config;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerConfigResolverTest {

    // Short interval so retry tests run fast; production uses the default 5s.
    private final BrokerConfigResolver resolver = new BrokerConfigResolver(Duration.ofMillis(50));

    @Test
    void resolvesBrokerFromRegistryConfigEndpoint() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/config", exchange -> {
            byte[] body = "{\"bootstrapServers\": \"kafka:9092\"}".getBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
            assertThat(resolver.resolve(baseUrl)).isEqualTo("kafka:9092");
            // Trailing slash tolerated.
            assertThat(resolver.resolve(baseUrl + "/")).isEqualTo("kafka:9092");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void retriesUntilRegistryBecomesAvailable() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/config", exchange -> {
            if (attempts.incrementAndGet() < 3) {
                exchange.sendResponseHeaders(503, -1);
                exchange.close();
                return;
            }
            byte[] body = "{\"bootstrapServers\": \"kafka:9092\"}".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            assertThat(resolver.resolve("http://127.0.0.1:" + server.getAddress().getPort()))
                    .isEqualTo("kafka:9092");
            assertThat(attempts.get()).isGreaterThanOrEqualTo(3);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void retriesUntilRegistryAdvertisesBroker() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/config", exchange -> {
            byte[] body = attempts.incrementAndGet() < 2
                    ? "{}".getBytes() // answered, but no broker advertised yet
                    : "{\"bootstrapServers\": \"kafka:9092\"}".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            assertThat(resolver.resolve("http://127.0.0.1:" + server.getAddress().getPort()))
                    .isEqualTo("kafka:9092");
            assertThat(attempts.get()).isGreaterThanOrEqualTo(2);
        } finally {
            server.stop(0);
        }
    }
}
