package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.schedule.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeContext {

    private static final Logger logger = LoggerFactory.getLogger(NodeContext.class);
    private final NodeId selfNodeId;
    private final NodeGroup nodeGroup;
    private final NodeStore nodeStore;

    private EventBus eventBus;
    private Log log;
    private Scheduler scheduler;
    private Connector connector;

    public NodeContext(NodeId selfNodeId, NodeGroup nodeGroup, NodeStore nodeStore) {
        this.selfNodeId = selfNodeId;
        this.nodeGroup = nodeGroup;
        this.nodeStore = nodeStore;
    }

    public NodeId getSelfNodeId() {
        return this.selfNodeId;
    }

    public NodeGroup getNodeGroup() {
        return nodeGroup;
    }

    public NodeStore getNodeStore() {
        return nodeStore;
    }

    public Log getLog() {
        return log;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public Connector getConnector() {
        return connector;
    }

    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
        this.eventBus.register(this);
    }

    public void setLog(Log log) {
        this.log = log;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public void register(Object eventSubscriber) {
        this.eventBus.register(eventSubscriber);
    }

    @Subscribe
    public void onReceive(DeadEvent deadEvent) {
        logger.warn("dead event {}", deadEvent);
    }

    public void release() throws InterruptedException {
        this.scheduler.stop();
        this.connector.release();
    }

}
