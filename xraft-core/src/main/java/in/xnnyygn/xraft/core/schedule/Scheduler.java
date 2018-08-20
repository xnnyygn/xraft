package in.xnnyygn.xraft.core.schedule;

import javax.annotation.Nonnull;

/**
 * Scheduler.
 */
// TODO optimize
public interface Scheduler {

    /**
     * Schedule log replication task.
     *
     * @param task task
     * @return log replication task
     */
    @Nonnull
    LogReplicationTask scheduleLogReplicationTask(@Nonnull Runnable task);

    /**
     * Schedule election timeout.
     *
     * @param task task
     * @return election timeout
     */
    @Nonnull
    ElectionTimeout scheduleElectionTimeout(@Nonnull Runnable task);

    /**
     * Stop scheduler.
     *
     * @throws InterruptedException if interrupted
     */
    void stop() throws InterruptedException;

}
