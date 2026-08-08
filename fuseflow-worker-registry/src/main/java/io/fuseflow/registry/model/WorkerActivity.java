package io.fuseflow.registry.model;

import java.util.UUID;

/** An activity name a worker advertises (capability) — a row in {@code registry.worker_activity}. */
public record WorkerActivity(UUID workerId, String activityName) {
}
