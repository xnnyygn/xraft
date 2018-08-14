package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

public class PeerReplicatingState extends AbstractReplicatingState {

    protected final NodeId nodeId;
    int nextIndex;
    int matchIndex = 0;

    public PeerReplicatingState(ReplicatingState replicatingState) {
        super(true);
        this.nodeId = replicatingState.getNodeId();
        this.nextIndex = replicatingState.getNextIndex();
        this.matchIndex = replicatingState.getMatchIndex();
    }

    public PeerReplicatingState(NodeId nodeId, int nextIndex) {
        super(true);
        this.nodeId = nodeId;
        this.nextIndex = nextIndex;
    }

    @Override
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

    public void setMatchIndex(int matchIndex) {
        this.matchIndex = matchIndex;
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
    public String toString() {
        return "PeerReplicatingState{nodeId=" + nodeId +
                ", matchIndex=" + matchIndex +
                ", nextIndex=" + nextIndex + '}';
    }

}
