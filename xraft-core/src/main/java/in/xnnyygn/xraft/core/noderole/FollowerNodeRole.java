package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.rpc.message.*;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.node.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class FollowerNodeRole extends AbstractNodeRole {

    private static final Logger logger = LoggerFactory.getLogger(FollowerNodeRole.class);
    private final NodeId votedFor;
    private final NodeId leaderId;
    private final ElectionTimeout electionTimeout;

    public FollowerNodeRole(NodeStore nodeStore, ElectionTimeout electionTimeout) {
        this(nodeStore.getCurrentTerm(), nodeStore.getVotedFor(), null, electionTimeout);
    }

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

    public static boolean isStableBetween(FollowerNodeRole before, FollowerNodeRole after) {
        return before.term == after.term && Objects.equals(before.votedFor, after.votedFor) && Objects.equals(before.leaderId, after.leaderId);
    }

    @Override
    public RoleStateSnapshot takeSnapshot() {
        RoleStateSnapshot snapshot = new RoleStateSnapshot(this.role, this.term);
        snapshot.setVotedFor(this.votedFor);
        snapshot.setLeaderId(this.leaderId);
        return snapshot;
    }

    @Override
    public void cancelTimeoutOrTask() {
        this.electionTimeout.cancel();
    }

    @Override
    protected RequestVoteResult processRequestVoteRpc(NodeRoleContext context, RequestVoteRpc rpc) {
        assert rpc.getTerm() == this.term;
        assert rpc.getCandidateId() != null;

        if ((this.votedFor == null && !context.getLog().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm())) ||
                Objects.equals(this.votedFor, rpc.getCandidateId())) {

            // vote for candidate
            context.changeToNodeRole(new FollowerNodeRole(rpc.getTerm(), rpc.getCandidateId(), null, electionTimeout.reset()));
            return new RequestVoteResult(rpc.getTerm(), true);
        }

        // 1. voted for other peer
        // 2. candidate's log is not up-to-date
        return new RequestVoteResult(rpc.getTerm(), false);
    }

    @Override
    protected AppendEntriesResult processAppendEntriesRpc(NodeRoleContext context, AppendEntriesRpc rpc) {
        assert rpc.getTerm() == this.term;

        context.changeToNodeRole(new FollowerNodeRole(this.term, this.votedFor, rpc.getLeaderId(), electionTimeout.reset()));
        return new AppendEntriesResult(this.term, context.getLog().appendEntries(rpc));
    }

    @Override
    protected void processInstallSnapshotRpc(NodeRoleContext context, InstallSnapshotRpcMessage rpcMessage) {
        context.getLog().installSnapshot(rpcMessage.get());
    }

    @Override
    public String toString() {
        return "FollowerNodeRole{" +
                electionTimeout +
                ", leaderId=" + leaderId +
                ", term=" + term +
                ", votedFor=" + votedFor +
                '}';
    }

}
