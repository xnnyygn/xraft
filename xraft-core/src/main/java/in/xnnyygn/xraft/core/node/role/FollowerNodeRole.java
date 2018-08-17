package in.xnnyygn.xraft.core.node.role;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

@Immutable
public class FollowerNodeRole extends AbstractNodeRole {

    private final NodeId votedFor;
    private final NodeId leaderId;
    private final ElectionTimeout electionTimeout;

    public FollowerNodeRole(int term, NodeId votedFor, NodeId leaderId, ElectionTimeout electionTimeout) {
        super(RoleName.FOLLOWER, term);
        this.votedFor = votedFor;
        this.leaderId = leaderId;
        this.electionTimeout = electionTimeout;
    }

    public NodeId getVotedFor() {
        return votedFor;
    }

    public NodeId getLeaderId() {
        return leaderId;
    }

    @Override
    public NodeId getLeaderId(NodeId selfId) {
        return leaderId;
    }

    @Override
    public void cancelTimeoutOrTask() {
        electionTimeout.cancel();
    }

    @Override
    public RoleState getState() {
        RoleState state = new RoleState(RoleName.FOLLOWER, term);
        state.setVotedFor(votedFor);
        state.setLeaderId(leaderId);
        return state;
    }

    @Override
    protected boolean doStateEquals(AbstractNodeRole role) {
        FollowerNodeRole that = (FollowerNodeRole) role;
        return Objects.equals(this.votedFor, that.votedFor) && Objects.equals(this.leaderId, that.leaderId);
    }

    @Override
    public String toString() {
        return "FollowerNodeRole{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                ", votedFor=" + votedFor +
                ", electionTimeout=" + electionTimeout +
                '}';
    }
}
