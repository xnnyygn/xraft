package in.xnnyygn.xraft.rpc;

import in.xnnyygn.xraft.node.RaftNodeId;

import java.io.Serializable;

public class RequestVoteRpc implements Serializable {

    private int term;
    private RaftNodeId candidateId;

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public RaftNodeId getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(RaftNodeId candidateId) {
        this.candidateId = candidateId;
    }

    @Override
    public String toString() {
        return "RequestVoteRpc{" + "candidateId=" + candidateId +
                ", term=" + term +
                '}';
    }
}
