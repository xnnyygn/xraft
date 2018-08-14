package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.node.NodeId;

public class RoleState {

    private final RoleName roleName;
    private final int term;
    private int votesCount;
    private NodeId votedFor;
    private NodeId leaderId;

    public RoleState(RoleName roleName, int term) {
        this.roleName = roleName;
        this.term = term;
    }

    public RoleName getRoleName() {
        return roleName;
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
        switch (this.roleName) {
            case FOLLOWER:
                return "Follower{term=" + this.term + ", votedFor=" + this.votedFor + ", leaderId=" + this.leaderId + "}";
            case CANDIDATE:
                return "Candidate{term=" + this.term + ", votesCount=" + this.votesCount + "}";
            case LEADER:
                return "Leader{term=" + this.term + "}";
            default:
                throw new IllegalStateException("unexpected node role name [" + this.roleName + "]");
        }
    }

}
