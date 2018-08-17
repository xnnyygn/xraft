package in.xnnyygn.xraft.core.node.role;

import in.xnnyygn.xraft.core.node.NodeId;

// TODO extract interface
// TODO add doc
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

    // TODO add constructors

    public RoleName getRoleName() {
        return roleName;
    }

    public int getTerm() {
        return term;
    }

    public int getVotesCount() {
        return votesCount;
    }

    public void setVotesCount(int votesCount) {
        this.votesCount = votesCount;
    }

    public NodeId getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(NodeId votedFor) {
        this.votedFor = votedFor;
    }

    public NodeId getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(NodeId leaderId) {
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
