package in.xnnyygn.xraft.core.node.role;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

@Immutable
public class FollowerNodeRole extends AbstractNodeRole {

    private final NodeId votedFor;
    private final NodeId leaderId;
    private final int preVotesCount;
    private final ElectionTimeout electionTimeout;
    private final long lastHeartbeat;

    public FollowerNodeRole(int term, NodeId votedFor, NodeId leaderId, int preVotesCount, long lastHeartbeat, ElectionTimeout electionTimeout) {
        super(RoleName.FOLLOWER, term);
        this.votedFor = votedFor;
        this.leaderId = leaderId;
        this.preVotesCount = preVotesCount;
        this.electionTimeout = electionTimeout;
        this.lastHeartbeat = lastHeartbeat;
    }

    public NodeId getVotedFor() {
        return votedFor;
    }

    public NodeId getLeaderId() {
        return leaderId;
    }

    public int getPreVotesCount() {
        return preVotesCount;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
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
        DefaultRoleState state = new DefaultRoleState(RoleName.FOLLOWER, term);
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
