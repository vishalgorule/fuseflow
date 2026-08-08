-- Phase 3: Worker registry tables (owned by the Worker Registry).
--
-- * worker            — one row per registered worker; liveness (status, last_heartbeat_at)
--                       is derived from heartbeats by the scheduled offline detector.
-- * worker_activity   — the activity names a worker advertises (capabilities, FR-4).
-- * worker_heartbeat  — append-only heartbeat log (purged by the cleanup job); the
--                       denormalized last_heartbeat_at on `worker` powers the offline
--                       detector without touching this log.

CREATE TABLE registry.worker (
    id                UUID PRIMARY KEY,
    host              TEXT NOT NULL,
    capacity          INT  NOT NULL DEFAULT 1,          -- max concurrent activities
    status            TEXT NOT NULL DEFAULT 'ONLINE',   -- ONLINE | DEGRADED | OFFLINE
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    version           BIGINT NOT NULL DEFAULT 0,        -- optimistic-lock column
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE registry.worker_activity (
    worker_id     UUID NOT NULL REFERENCES registry.worker (id) ON DELETE CASCADE,
    activity_name TEXT NOT NULL,
    PRIMARY KEY (worker_id, activity_name)
);
-- Capability lookup: which workers support a given activity name (FR-12).
CREATE INDEX idx_worker_activity_name ON registry.worker_activity (activity_name);

CREATE TABLE registry.worker_heartbeat (
    id          BIGSERIAL PRIMARY KEY,   -- deterministic append order
    worker_id   UUID NOT NULL REFERENCES registry.worker (id) ON DELETE CASCADE,
    capacity    INT,                     -- capacity reported with this heartbeat (may be null)
    received_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_worker_heartbeat_worker ON registry.worker_heartbeat (worker_id, id);
