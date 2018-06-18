package in.xnnyygn.xraft.core.schedule;

import in.xnnyygn.xraft.core.server.ServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ElectionTimeout {

    private static final Logger logger = LoggerFactory.getLogger(ElectionTimeout.class);
    private final ScheduledFuture<?> scheduledFuture;
    private final ElectionTimeoutScheduler schedulerCallback;
    private final ServerId selfServerId;

    public ElectionTimeout(ScheduledFuture<?> scheduledFuture, ElectionTimeoutScheduler schedulerCallback, ServerId selfServerId) {
        this.scheduledFuture = scheduledFuture;
        this.schedulerCallback = schedulerCallback;
        this.selfServerId = selfServerId;
    }

    public void cancel() {
        logger.debug("Server {}, cancel election timeout", this.selfServerId);
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
