package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.server.ServerId;

import java.io.Serializable;

public class RequestVoteRpc implements Serializable {

    private int term;
    private ServerId candidateId;

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public ServerId getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(ServerId candidateId) {
        this.candidateId = candidateId;
    }

    @Override
    public String toString() {
        return "RequestVoteRpc{" + "candidateId=" + candidateId +
                ", term=" + term +
                '}';
    }
}
