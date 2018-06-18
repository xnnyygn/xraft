package in.xnnyygn.xraft.core.schedule;

import in.xnnyygn.xraft.core.server.ServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class LogReplicationTask {

    private static final Logger logger = LoggerFactory.getLogger(LogReplicationTask.class);
    private final ScheduledFuture<?> scheduledFuture;
    private final ServerId selfServerId;

    public LogReplicationTask(ScheduledFuture<?> scheduledFuture, ServerId selfServerId) {
        this.scheduledFuture = scheduledFuture;
        this.selfServerId = selfServerId;
    }

    public void cancel() {
        logger.debug("Server {}, cancel log replication task", this.selfServerId);
        this.scheduledFuture.cancel(false);
    }

    @Override
    public String toString() {
        return "LogReplicationTask{delay=" + scheduledFuture.getDelay(TimeUnit.MILLISECONDS) + "}";
    }

}
