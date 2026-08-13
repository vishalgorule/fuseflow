package io.fuseflow.sample;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Verifies the sample workers application context boots. The SDK worker runtime is disabled so
 * the test needs no Kafka or registry; the full stack is exercised via the manual Phase 4 demo
 * (README) and the SDK's own unit tests.
 */
@SpringBootTest(properties = "fuseflow.worker.enabled=false")
class SampleWorkersApplicationTests {

    @Test
    void contextLoads() {
    }
}
