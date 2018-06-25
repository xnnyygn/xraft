package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.node.NodeId;

public class ReplicationState {

    private final NodeId nodeId;
    private int nextIndex;
    private int matchIndex = 0;
    private volatile boolean replicating = false;

    public ReplicationState(NodeId nodeId, int nextIndex) {
        this.nodeId = nodeId;
        this.nextIndex = nextIndex;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public int getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(int nextIndex) {
        this.nextIndex = nextIndex;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(int matchIndex) {
        this.matchIndex = matchIndex;
    }

    public void backOffNextIndex() {
        // next index must >= 1
        if (this.nextIndex > 1) {
            this.nextIndex--;
        }
    }

    public void setReplicating(boolean replicating) {
        this.replicating = replicating;
    }

    public boolean isReplicating() {
        return this.replicating;
    }

    public void advance(int lastEntryIndex) {
        this.matchIndex = lastEntryIndex;
        this.nextIndex = lastEntryIndex + 1;
    }

    @Override
    public String toString() {
        return "ReplicationState{" +
                "matchIndex=" + matchIndex +
                ", nextIndex=" + nextIndex +
                ", nodeId=" + nodeId +
                ", replicating=" + replicating +
                '}';
    }

}
