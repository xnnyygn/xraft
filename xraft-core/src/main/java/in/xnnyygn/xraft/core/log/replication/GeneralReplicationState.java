package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

public class GeneralReplicationState extends AbstractReplicationState {

    private final NodeId nodeId;
    private int nextIndex;
    private int matchIndex = 0;

    public GeneralReplicationState(NodeId nodeId, int nextIndex) {
        this(nodeId, nextIndex, true);
    }

    public GeneralReplicationState(NodeId nodeId, int nextIndex, boolean member) {
        super(member);
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
    public void backOffNextIndex() {
        if (this.nextIndex > 1) {
            this.nextIndex--;
        }
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
        return "GeneralReplicationState{" +
                "matchIndex=" + matchIndex +
                ", nextIndex=" + nextIndex +
                ", nodeId=" + nodeId +
                '}';
    }

}
