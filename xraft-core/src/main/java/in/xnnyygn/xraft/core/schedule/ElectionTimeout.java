package in.xnnyygn.xraft.core.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ElectionTimeout {

    private static final Logger logger = LoggerFactory.getLogger(ElectionTimeout.class);
    public static final ElectionTimeout NONE = new ElectionTimeout(new NullScheduledFuture(), new NullElectionTimeoutScheduler());

    private final ScheduledFuture<?> scheduledFuture;
    private final ElectionTimeoutScheduler schedulerCallback;

    public ElectionTimeout(ScheduledFuture<?> scheduledFuture, ElectionTimeoutScheduler schedulerCallback) {
        this.scheduledFuture = scheduledFuture;
        this.schedulerCallback = schedulerCallback;
    }

    public void cancel() {
        logger.debug("cancel election timeout");
        this.scheduledFuture.cancel(false);
    }

    public ElectionTimeout reset() {
        this.cancel();
        return schedulerCallback.scheduleElectionTimeout();
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
