package in.xnnyygn.xraft.core.schedule;

import in.xnnyygn.xraft.core.server.ServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.*;

public class Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
    private final Random electionTimeoutRandom;
    private final ScheduledExecutorService scheduledExecutor;
    private final ServerId selfServerId;

    public Scheduler(ServerId selfServerId) {
        this.electionTimeoutRandom = new Random();
        this.selfServerId = selfServerId;
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "scheduler-" + this.selfServerId));
    }

    public LogReplicationTask scheduleLogReplicationTask(Runnable task) {
        logger.debug("Server {}, schedule log replication task", this.selfServerId);
        ScheduledFuture<?> scheduledFuture = this.scheduledExecutor.scheduleWithFixedDelay(
                task, 0, 1000, TimeUnit.MILLISECONDS);
        return new LogReplicationTask(scheduledFuture, this.selfServerId);
    }

    public ElectionTimeout scheduleElectionTimeout(Runnable task) {
        logger.debug("Server {}, schedule election timeout", this.selfServerId);
        int timeout = electionTimeoutRandom.nextInt(2000) + 3000;
        ScheduledFuture<?> scheduledFuture = scheduledExecutor.schedule(task, timeout, TimeUnit.MILLISECONDS);
        return new ElectionTimeout(scheduledFuture, () -> scheduleElectionTimeout(task), this.selfServerId);
    }

    public void stop() throws InterruptedException {
        logger.debug("Server {}, stop scheduler", this.selfServerId);
        this.scheduledExecutor.shutdown();
        this.scheduledExecutor.awaitTermination(1, TimeUnit.SECONDS);
    }

}
