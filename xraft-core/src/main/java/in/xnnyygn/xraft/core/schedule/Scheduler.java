package in.xnnyygn.xraft.core.schedule;

import in.xnnyygn.xraft.core.node.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
    private final Random electionTimeoutRandom;
    private final ScheduledExecutorService scheduledExecutor;

    public Scheduler(NodeId selfNodeId) {
        this.electionTimeoutRandom = new Random();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "scheduler-" + selfNodeId));
    }

    public LogReplicationTask scheduleLogReplicationTask(Runnable task) {
        logger.debug("schedule log replication task");
        ScheduledFuture<?> scheduledFuture = this.scheduledExecutor.scheduleWithFixedDelay(
                task, 0, 1000, TimeUnit.MILLISECONDS);
        return new LogReplicationTask(scheduledFuture);
    }

    public ElectionTimeout scheduleElectionTimeout(Runnable task) {
        logger.debug("schedule election timeout");
        int timeout = electionTimeoutRandom.nextInt(2000) + 3000;
        ScheduledFuture<?> scheduledFuture = scheduledExecutor.schedule(task, timeout, TimeUnit.MILLISECONDS);
        return new ElectionTimeout(scheduledFuture, () -> scheduleElectionTimeout(task));
    }

    public void stop() throws InterruptedException {
        logger.debug("stop scheduler");
        this.scheduledExecutor.shutdown();
        this.scheduledExecutor.awaitTermination(1, TimeUnit.SECONDS);
    }

}
