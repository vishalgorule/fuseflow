package io.fuseflow.engine.model;

/**
 * Terminal/non-terminal lifecycle states of a workflow execution.
 *
 * <p>Phase 2 set: {@code RUNNING} → {@code COMPLETED | FAILED}. Phase 8 adds lifecycle
 * operations: {@code PAUSED} (non-terminal — scheduling is suspended, in-flight activities
 * drain) and {@code CANCELLED} (terminal — the operator aborted the execution).
 *
 * <p>Terminal states: {@link #COMPLETED}, {@link #FAILED}, {@link #CANCELLED}.
 */
public enum WorkflowStatus {
    RUNNING,
    PAUSED,
    COMPLETED,
    FAILED,
    CANCELLED
}
