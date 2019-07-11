package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;

public class AddNodeTask extends AbstractGroupConfigChangeTask {

    private final NodeEndpoint endpoint;
    private final int nextIndex;
    private final int matchIndex;

    public AddNodeTask(GroupConfigChangeTaskContext context, NodeEndpoint endpoint, NewNodeCatchUpTaskResult newNodeCatchUpTaskResult) {
        this(context, endpoint, newNodeCatchUpTaskResult.getNextIndex(), newNodeCatchUpTaskResult.getMatchIndex());
    }

    public AddNodeTask(GroupConfigChangeTaskContext context, NodeEndpoint endpoint, int nextIndex, int matchIndex) {
        super(context);
        this.endpoint = endpoint;
        this.nextIndex = nextIndex;
        this.matchIndex = matchIndex;
    }

    @Override
    protected void appendGroupConfig() {
        context.addNode(endpoint, nextIndex, matchIndex);
    }

    @Override
    protected synchronized void doOnLogCommitted(GroupConfigEntry entry) {
        if (state != State.GROUP_CONFIG_APPENDED) {
            throw new IllegalStateException("log committed before log appended");
        }
        setState(State.GROUP_CONFIG_COMMITTED);
        notify();
    }

    @Override
    public String toString() {
        return "AddNodeTask{" +
                "state=" + state +
                ", endpoint=" + endpoint +
                ", nextIndex=" + nextIndex +
                ", matchIndex=" + matchIndex +
                '}';
    }

}
