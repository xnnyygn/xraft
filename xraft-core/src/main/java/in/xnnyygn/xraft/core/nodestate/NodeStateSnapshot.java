package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.server.ServerId;

public class NodeStateSnapshot {

    private final NodeRole role;
    private final int term;
    private int votesCount;
    private ServerId votedFor;
    private ServerId leaderId;

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

    public ServerId getVotedFor() {
        return votedFor;
    }

    void setVotedFor(ServerId votedFor) {
        this.votedFor = votedFor;
    }

    public ServerId getLeaderId() {
        return leaderId;
    }

    void setLeaderId(ServerId leaderId) {
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
                throw new IllegalStateException("unexpected server role [" + this.role + "]");
        }
    }

}
