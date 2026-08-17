package io.fuseflow.engine;

import io.fuseflow.engine.config.ReliabilityProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(ReliabilityProperties.class)
public class FuseflowWorkflowEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(FuseflowWorkflowEngineApplication.class, args);
    }
}
