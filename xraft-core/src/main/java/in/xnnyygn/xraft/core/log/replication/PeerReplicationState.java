package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

public class PeerReplicationState extends AbstractReplicationState {

    protected final NodeId nodeId;
    int nextIndex;
    int matchIndex = 0;

    public PeerReplicationState(ReplicationState replicationState) {
        super(true);
        this.nodeId = replicationState.getNodeId();
        this.nextIndex = replicationState.getNextIndex();
        this.matchIndex = replicationState.getMatchIndex();
    }

    public PeerReplicationState(NodeId nodeId, int nextIndex) {
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
        return "PeerReplicationState{nodeId=" + nodeId +
                "matchIndex=" + matchIndex +
                ", nextIndex=" + nextIndex + '}';
    }

}
