package in.xnnyygn.xraft.rpc;

import in.xnnyygn.xraft.node.RaftNodeId;

import java.io.Serializable;

public class AppendEntriesRpc implements Serializable {

    private int term;
    private RaftNodeId leaderId;

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public RaftNodeId getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(RaftNodeId leaderId) {
        this.leaderId = leaderId;
    }

    @Override
    public String toString() {
        return "AppendEntriesRpc{" +
                "leaderId=" + leaderId +
                ", term=" + term +
                '}';
    }

}
