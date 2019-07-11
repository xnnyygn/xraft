package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.node.NodeId;

public class RemoveNodeTask extends AbstractGroupConfigChangeTask {

    private final NodeId nodeId;
    private final NodeId selfId;

    public RemoveNodeTask(GroupConfigChangeTaskContext context, NodeId nodeId, NodeId selfId) {
        super(context);
        this.nodeId = nodeId;
        this.selfId = selfId;
    }

    @Override
    protected void appendGroupConfig() {
        context.removeNode(nodeId);
    }

    @Override
    protected synchronized void doOnLogCommitted(GroupConfigEntry entry) {
        if (state != State.GROUP_CONFIG_APPENDED) {
            throw new IllegalStateException("log committed before log appended");
        }
        setState(State.GROUP_CONFIG_COMMITTED);
        if (nodeId.equals(selfId)) {
            context.downgradeSelf();
        }
        notify();
    }

    @Override
    public String toString() {
        return "RemoveNodeTask{" +
                "nodeId=" + nodeId +
                '}';
    }

}
