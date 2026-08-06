package io.fuseflow.engine.dto;

import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/** One immutable event from an execution's history (FR-9). */
public record EventResponse(long id, UUID executionId, String eventType, JsonNode payload, Instant createdAt) {
}
