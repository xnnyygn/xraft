package in.xnnyygn.xraft.core.schedule;

import com.google.common.base.Preconditions;
import in.xnnyygn.xraft.core.node.config.NodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@ThreadSafe
public class DefaultScheduler implements Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(DefaultScheduler.class);
    private final int minElectionTimeout;
    private final int maxElectionTimeout;
    private final int logReplicationDelay;
    private final int logReplicationInterval;
    private final Random electionTimeoutRandom;
    private final ScheduledExecutorService scheduledExecutorService;

    public DefaultScheduler(NodeConfig config) {
        this(config.getMinElectionTimeout(), config.getMaxElectionTimeout(), config.getLogReplicationDelay(),
                config.getLogReplicationInterval());
    }

    public DefaultScheduler(int minElectionTimeout, int maxElectionTimeout, int logReplicationDelay, int logReplicationInterval) {
        if (minElectionTimeout <= 0 || maxElectionTimeout <= 0 || minElectionTimeout > maxElectionTimeout) {
            throw new IllegalArgumentException("election timeout should not be 0 or min > max");
        }
        if (logReplicationDelay < 0 || logReplicationInterval <= 0) {
            throw new IllegalArgumentException("log replication delay < 0 or log replication interval <= 0");
        }
        this.minElectionTimeout = minElectionTimeout;
        this.maxElectionTimeout = maxElectionTimeout;
        this.logReplicationDelay = logReplicationDelay;
        this.logReplicationInterval = logReplicationInterval;
        electionTimeoutRandom = new Random();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "scheduler"));
    }

    @Override
    @Nonnull
    public LogReplicationTask scheduleLogReplicationTask(@Nonnull Runnable task) {
        Preconditions.checkNotNull(task);
        logger.debug("schedule log replication task");
        ScheduledFuture<?> scheduledFuture = this.scheduledExecutorService.scheduleWithFixedDelay(
                task, logReplicationDelay, logReplicationInterval, TimeUnit.MILLISECONDS);
        return new LogReplicationTask(scheduledFuture);
    }

    @Override
    @Nonnull
    public ElectionTimeout scheduleElectionTimeout(@Nonnull Runnable task) {
        Preconditions.checkNotNull(task);
        logger.debug("schedule election timeout");
        int timeout = electionTimeoutRandom.nextInt(maxElectionTimeout - minElectionTimeout) + minElectionTimeout;
        ScheduledFuture<?> scheduledFuture = scheduledExecutorService.schedule(task, timeout, TimeUnit.MILLISECONDS);
        return new ElectionTimeout(scheduledFuture);
    }

    @Override
    public void stop() throws InterruptedException {
        logger.debug("stop scheduler");
        scheduledExecutorService.shutdown();
        scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS);
    }

}
