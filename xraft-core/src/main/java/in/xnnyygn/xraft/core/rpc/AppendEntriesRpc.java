package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeId;

import java.io.Serializable;

public class AppendEntriesRpc implements Serializable {

    private int term;
    private NodeId leaderId;

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public NodeId getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(NodeId leaderId) {
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
