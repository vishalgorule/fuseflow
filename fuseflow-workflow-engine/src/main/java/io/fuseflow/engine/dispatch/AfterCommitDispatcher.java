package io.fuseflow.engine.dispatch;

import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Runs an action only after the surrounding transaction commits, so executor threads never
 * observe uncommitted state (persist → append event → publish, architecture §10.1). When no
 * transaction is active (e.g. boot-time recovery), the action runs immediately.
 */
@Component
public class AfterCommitDispatcher {

    public void runAfterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    action.run();
                }
            });
        } else {
            action.run();
        }
    }
}
