package in.xnnyygn.xraft.node;

public class RaftNodeSave {

    private int currentTerm = 0;
    private RaftNodeId votedFor = null;

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public RaftNodeId getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(RaftNodeId votedFor) {
        this.votedFor = votedFor;
    }

}
