package in.xnnyygn.xraft.schedule;

import in.xnnyygn.xraft.server.ServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class LogReplicationTask {

    private static final Logger logger = LoggerFactory.getLogger(LogReplicationTask.class);
    private final ScheduledFuture<?> scheduledFuture;
    private final ServerId selfNodeId;

    public LogReplicationTask(ScheduledFuture<?> scheduledFuture, ServerId selfNodeId) {
        this.scheduledFuture = scheduledFuture;
        this.selfNodeId = selfNodeId;
    }

    public void cancel() {
        logger.debug("Node {}, cancel log replication task", this.selfNodeId);
        this.scheduledFuture.cancel(false);
    }

    @Override
    public String toString() {
        return "LogReplicationTask{delay=" + scheduledFuture.getDelay(TimeUnit.MILLISECONDS) + "}";
    }

}
