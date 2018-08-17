package in.xnnyygn.xraft.core.node.replication;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.node.NodeId;

public class SelfReplicatingState implements ReplicatingState {

    private final NodeId selfNodeId;
    private final Log log;

    public SelfReplicatingState(NodeId selfNodeId, Log log) {
        this.selfNodeId = selfNodeId;
        this.log = log;
    }

    @Override
    public NodeId getNodeId() {
        return selfNodeId;
    }

    @Override
    public int getNextIndex() {
        return log.getNextIndex();
    }

    @Override
    public int getMatchIndex() {
        return log.getNextIndex() - 1;
    }

    @Override
    public boolean backOffNextIndex() {
        throw new UnsupportedOperationException("self");
    }

    @Override
    public boolean advance(int lastEntryIndex) {
        throw new UnsupportedOperationException("self");
    }

    @Override
    public boolean isTarget() {
        return false;
    }

    @Override
    public boolean isReplicating() {
        throw new UnsupportedOperationException("self");
    }

    @Override
    public long getLastReplicatedAt() {
        throw new UnsupportedOperationException("self");
    }

    @Override
    public void setReplicating(boolean replicating) {
        throw new UnsupportedOperationException("self");
    }

    @Override
    public void setLastReplicatedAt(long lastReplicatedAt) {
        throw new UnsupportedOperationException("self");
    }

}
