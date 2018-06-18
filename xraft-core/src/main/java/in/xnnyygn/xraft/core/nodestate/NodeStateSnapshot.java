package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.node.NodeId;

public class NodeStateSnapshot {

    private final NodeRole role;
    private final int term;
    private int votesCount;
    private NodeId votedFor;
    private NodeId leaderId;

    public NodeStateSnapshot(NodeRole role, int term) {
        this.role = role;
        this.term = term;
    }

    public NodeRole getRole() {
        return role;
    }

    public int getTerm() {
        return term;
    }

    public int getVotesCount() {
        return votesCount;
    }

    void setVotesCount(int votesCount) {
        this.votesCount = votesCount;
    }

    public NodeId getVotedFor() {
        return votedFor;
    }

    void setVotedFor(NodeId votedFor) {
        this.votedFor = votedFor;
    }

    public NodeId getLeaderId() {
        return leaderId;
    }

    void setLeaderId(NodeId leaderId) {
        this.leaderId = leaderId;
    }

    public String toString() {
        switch (this.role) {
            case FOLLOWER:
                return "Follower{term=" + this.term + ", votedFor=" + this.votedFor + ", leaderId=" + this.leaderId + "}";
            case CANDIDATE:
                return "Candidate{term=" + this.term + ", votesCount=" + this.votesCount + "}";
            case LEADER:
                return "Leader{term=" + this.term + "}";
            default:
                throw new IllegalStateException("unexpected node role [" + this.role + "]");
        }
    }

}
