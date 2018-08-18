package in.xnnyygn.xraft.core.node.replication;

import in.xnnyygn.xraft.core.node.NodeId;

import javax.annotation.Nonnull;

// TODO add test
public class PeerReplicatingState implements ReplicatingState {

    protected final NodeId nodeId;
    private int nextIndex;
    private int matchIndex;
    private boolean replicating = false;
    private long lastReplicatedAt = 0;

    public PeerReplicatingState(NodeId nodeId, int nextIndex) {
        this(nodeId, nextIndex, 0);
    }

    public PeerReplicatingState(NodeId nodeId, int nextIndex, int matchIndex) {
        this.nodeId = nodeId;
        this.nextIndex = nextIndex;
        this.matchIndex = matchIndex;
    }

    // TODO remove me
    @Override
    @Nonnull
    public NodeId getNodeId() {
        return nodeId;
    }

    @Override
    public int getNextIndex() {
        return nextIndex;
    }

    @Override
    public int getMatchIndex() {
        return matchIndex;
    }

    @Override
    public boolean backOffNextIndex() {
        if (this.nextIndex > 1) {
            this.nextIndex--;
            return true;
        }
        return false;
    }

    @Override
    public boolean advance(int lastEntryIndex) {
        // changed
        boolean result = (this.matchIndex != lastEntryIndex || this.nextIndex != (lastEntryIndex + 1));

        this.matchIndex = lastEntryIndex;
        this.nextIndex = lastEntryIndex + 1;

        return result;
    }

    @Override
    public boolean isTarget() {
        return true;
    }

    @Override
    public boolean isReplicating() {
        return replicating;
    }

    @Override
    public long getLastReplicatedAt() {
        return lastReplicatedAt;
    }

    @Override
    public void setReplicating(boolean replicating) {
        this.replicating = replicating;
    }

    @Override
    public void setLastReplicatedAt(long lastReplicatedAt) {
        this.lastReplicatedAt = lastReplicatedAt;
    }

    @Override
    public String toString() {
        return "PeerReplicatingState{nodeId=" + nodeId +
                ", matchIndex=" + matchIndex +
                ", nextIndex=" + nextIndex + '}';
    }

}
