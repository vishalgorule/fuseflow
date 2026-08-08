package io.fuseflow.registry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Worker Registry (port 8083): registration, heartbeats, offline detection (Phase 3). */
@SpringBootApplication
@EnableScheduling
public class FuseflowWorkerRegistryApplication {

    public static void main(String[] args) {
        SpringApplication.run(FuseflowWorkerRegistryApplication.class, args);
    }
}
