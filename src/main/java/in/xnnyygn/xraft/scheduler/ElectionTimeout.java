package in.xnnyygn.xraft.scheduler;

import in.xnnyygn.xraft.node.RaftNodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ElectionTimeout {

    private static final Logger logger = LoggerFactory.getLogger(ElectionTimeout.class);
    private final ScheduledFuture<?> scheduledFuture;
    private final ElectionTimeoutScheduler electionTimeoutScheduler;
    private final RaftNodeId selfNodeId;

    public ElectionTimeout(ScheduledFuture<?> scheduledFuture, ElectionTimeoutScheduler electionTimeoutScheduler, RaftNodeId selfNodeId) {
        this.scheduledFuture = scheduledFuture;
        this.electionTimeoutScheduler = electionTimeoutScheduler;
        this.selfNodeId = selfNodeId;
    }

    public void cancel() {
        logger.debug("Node {}, cancel election timeout", this.selfNodeId);
        this.scheduledFuture.cancel(false);
    }

    public ElectionTimeout reset() {
        this.cancel();
        return electionTimeoutScheduler.scheduleElectionTimeout();
    }

    @Override
    public String toString() {
        if (this.scheduledFuture.isCancelled()) {
            return "ElectionTimeout(state=cancelled)";
        }
        if (this.scheduledFuture.isDone()) {
            return "ElectionTimeout(state=done)";
        }
        return "ElectionTimeout{delay=" + scheduledFuture.getDelay(TimeUnit.MILLISECONDS) + "ms}";
    }

}
