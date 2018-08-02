package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

public class NewNodeReplicationState extends PeerReplicationState {

    private static final int MAX_ROUND = 10;
    private final long startTime;
    private int round = 1;

    public NewNodeReplicationState(NodeId nodeId, int nextIndex) {
        super(nodeId, nextIndex);
        this.startTime = System.currentTimeMillis();
    }

    public void increaseRound() {
        round++;
    }

    public boolean roundExceedOrTimeout(long timeout) {
        return (System.currentTimeMillis() - startTime > timeout) || round > MAX_ROUND;
    }

    @Override
    public String toString() {
        return "NewNodeReplicationState{" +
                "nodeId=" + nodeId +
                ", matchIndex=" + matchIndex +
                ", nextIndex=" + nextIndex +
                ", round=" + round +
                '}';
    }

}
