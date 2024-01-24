package in.xnnyygn.xraft.core.rpc.message;

import java.io.Serializable;
import in.xnnyygn.xraft.core.node.NodeId;

public class RequestVoteResult implements Serializable {

    private int term;
    private boolean voteGranted;
    private NodeId replyNode;

    public RequestVoteResult(int term, boolean voteGranted, NodeId replyNode) {
        this.term = term;
        this.voteGranted = voteGranted;
        this.replyNode = replyNode;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    public NodeId getReplyNode() {
        return replyNode;
    }

    @Override
    public String toString() {
        return "RequestVoteResult{" + "term=" + term +
                ", voteGranted=" + voteGranted +
                '}';
    }
}
