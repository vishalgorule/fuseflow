package io.fuseflow.common.messaging;

/**
 * The kinds of signals a worker can send about an activity on the {@code activity-results}
 * topic. {@code STARTED} is a lightweight progress signal (worker picked the task up) so the
 * engine can emit the {@code ActivityStarted} event; {@code COMPLETED}/{@code FAILED} are the
 * terminal outcomes fed into the engine's result handler.
 */
public enum ActivityResultType {
    STARTED,
    COMPLETED,
    FAILED
}
