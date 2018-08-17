package in.xnnyygn.xraft.core.node.role;

import in.xnnyygn.xraft.core.node.NodeId;

import javax.annotation.Nonnull;

/**
 * Default role state.
 */
public class DefaultRoleState implements RoleState {

    private final RoleName roleName;
    private final int term;
    private int votesCount = VOTES_COUNT_NOT_SET;
    private NodeId votedFor;
    private NodeId leaderId;

    public DefaultRoleState(RoleName roleName, int term) {
        this.roleName = roleName;
        this.term = term;
    }

    @Override
    @Nonnull
    public RoleName getRoleName() {
        return roleName;
    }

    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public int getVotesCount() {
        return votesCount;
    }

    public void setVotesCount(int votesCount) {
        this.votesCount = votesCount;
    }

    @Override
    public NodeId getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(NodeId votedFor) {
        this.votedFor = votedFor;
    }

    @Override
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
