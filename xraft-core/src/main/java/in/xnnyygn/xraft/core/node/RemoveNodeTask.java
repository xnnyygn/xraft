package in.xnnyygn.xraft.core.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveNodeTask implements GroupConfigChangeTask {

    private static final Logger logger = LoggerFactory.getLogger(RemoveNodeTask.class);
    private final GroupConfigChangeTaskContext context;
    private final NodeId nodeId;
    private final long startTime;

    public RemoveNodeTask(GroupConfigChangeTaskContext context, NodeId nodeId) {
        this.context = context;
        this.nodeId = nodeId;
        startTime = System.currentTimeMillis();
    }

    @Override
    public boolean isTargetNode(NodeId nodeId) {
        return this.nodeId.equals(nodeId);
    }

    @Override
    public long getStartTime() {
        return startTime;
    }

    @Override
    public synchronized GroupConfigChangeTaskResult call() throws Exception {
        logger.info("task start");
        context.downgradeNode(nodeId);
        wait();
        logger.info("task done");
        context.taskDone();
        return GroupConfigChangeTaskResult.OK;
    }

    @Override
    public synchronized void onLogCommitted() {
        logger.debug("log committed");
        context.removeNode(nodeId);
        notify();
    }

    @Override
    public String toString() {
        return "RemoveNodeTask{" +
                "nodeId=" + nodeId +
                '}';
    }

}
