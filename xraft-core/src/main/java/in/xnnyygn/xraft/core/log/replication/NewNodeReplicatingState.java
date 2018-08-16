package in.xnnyygn.xraft.core.log.replication;

import in.xnnyygn.xraft.core.node.NodeId;

@Deprecated
public class NewNodeReplicatingState extends PeerReplicatingState {

    private final long startTime;
    private int round = 1;

    public NewNodeReplicatingState(NodeId nodeId, int nextIndex) {
        super(nodeId, nextIndex);
        this.startTime = System.currentTimeMillis();
    }

    public void increaseRound() {
        round++;
    }

    public boolean roundExceedOrTimeout(int maxRound, long timeout) {
        return (System.currentTimeMillis() - startTime > timeout) || round > maxRound;
    }

    @Override
    public String toString() {
        return "NewNodeReplicatingState{" +
                "nodeId=" + nodeId +
                ", matchIndex=" + matchIndex +
                ", nextIndex=" + nextIndex +
                ", round=" + round +
                '}';
    }

}
