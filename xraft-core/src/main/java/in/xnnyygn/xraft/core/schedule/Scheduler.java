package in.xnnyygn.xraft.core.schedule;

import in.xnnyygn.xraft.core.node.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.*;

public class Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
    private final Random electionTimeoutRandom;
    private final ScheduledExecutorService scheduledExecutor;
    private final NodeId selfNodeId;

    // TODO remove node id from constructor
    public Scheduler(NodeId selfNodeId) {
        this.electionTimeoutRandom = new Random();
        this.selfNodeId = selfNodeId;
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "scheduler-" + this.selfNodeId));
    }

    public LogReplicationTask scheduleLogReplicationTask(Runnable task) {
        logger.debug("Node {}, schedule log replication task", this.selfNodeId);
        ScheduledFuture<?> scheduledFuture = this.scheduledExecutor.scheduleWithFixedDelay(
                task, 0, 1000, TimeUnit.MILLISECONDS);
        return new LogReplicationTask(scheduledFuture);
    }

    public ElectionTimeout scheduleElectionTimeout(Runnable task) {
        logger.debug("Node {}, schedule election timeout", this.selfNodeId);
        int timeout = electionTimeoutRandom.nextInt(2000) + 3000;
        ScheduledFuture<?> scheduledFuture = scheduledExecutor.schedule(task, timeout, TimeUnit.MILLISECONDS);
        return new ElectionTimeout(scheduledFuture, () -> scheduleElectionTimeout(task));
    }

    public void stop() throws InterruptedException {
        logger.debug("Node {}, stop scheduler", this.selfNodeId);
        this.scheduledExecutor.shutdown();
        this.scheduledExecutor.awaitTermination(1, TimeUnit.SECONDS);
    }

}
