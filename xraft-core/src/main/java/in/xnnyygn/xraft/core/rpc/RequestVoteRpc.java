package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeId;

import java.io.Serializable;

public class RequestVoteRpc implements Serializable {

    private int term;
    private NodeId candidateId;

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public NodeId getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(NodeId candidateId) {
        this.candidateId = candidateId;
    }

    @Override
    public String toString() {
        return "RequestVoteRpc{" + "candidateId=" + candidateId +
                ", term=" + term +
                '}';
    }
}
