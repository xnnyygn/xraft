package in.xnnyygn.xraft.core.node;

public class MemoryNodeStore implements NodeStore {

    private int currentTerm = 0;
    private NodeId votedFor = null;

    @Override
    public int getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    @Override
    public NodeId getVotedFor() {
        return votedFor;
    }

    @Override
    public void setVotedFor(NodeId votedFor) {
        this.votedFor = votedFor;
    }

    @Override
    public void close() {
    }

}
