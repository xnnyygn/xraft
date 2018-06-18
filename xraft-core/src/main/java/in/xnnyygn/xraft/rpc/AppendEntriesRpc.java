package in.xnnyygn.xraft.rpc;

import in.xnnyygn.xraft.server.ServerId;

import java.io.Serializable;

public class AppendEntriesRpc implements Serializable {

    private int term;
    private ServerId leaderId;

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public ServerId getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(ServerId leaderId) {
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
