package in.xnnyygn.xraft.server;

public class RaftNodeSave {

    private int currentTerm = 0;
    private ServerId votedFor = null;

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public ServerId getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(ServerId votedFor) {
        this.votedFor = votedFor;
    }

}
