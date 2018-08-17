package in.xnnyygn.xraft.core.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class NullScheduler implements Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(NullScheduler.class);

    @Override
    @Nonnull
    public LogReplicationTask scheduleLogReplicationTask(@Nonnull Runnable task) {
        logger.debug("schedule log replication task");
        return LogReplicationTask.NONE;
    }

    @Override
    @Nonnull
    public ElectionTimeout scheduleElectionTimeout(@Nonnull Runnable task) {
        logger.debug("schedule election timeout");
        return ElectionTimeout.NONE;
    }

    @Override
    public void stop() throws InterruptedException {
    }

}
