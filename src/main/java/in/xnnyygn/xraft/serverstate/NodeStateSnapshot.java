package in.xnnyygn.xraft.serverstate;

import in.xnnyygn.xraft.node.RaftNodeId;

public class NodeStateSnapshot {

    private final NodeRole role;
    private final int term;
    private int votesCount;
    private RaftNodeId votedFor;
    private RaftNodeId leaderId;

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

    public RaftNodeId getVotedFor() {
        return votedFor;
    }

    void setVotedFor(RaftNodeId votedFor) {
        this.votedFor = votedFor;
    }

    public RaftNodeId getLeaderId() {
        return leaderId;
    }

    void setLeaderId(RaftNodeId leaderId) {
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
