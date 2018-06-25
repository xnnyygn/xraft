package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.node.NodeStore;
import in.xnnyygn.xraft.core.rpc.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class FollowerNodeState extends AbstractNodeState {

    private static final Logger logger = LoggerFactory.getLogger(FollowerNodeState.class);
    private final NodeId votedFor;
    private final NodeId leaderId;
    private final ElectionTimeout electionTimeout;

    public FollowerNodeState(NodeStore nodeStore, ElectionTimeout electionTimeout) {
        this(nodeStore.getCurrentTerm(), nodeStore.getVotedFor(), null, electionTimeout);
    }

    public FollowerNodeState(int term, NodeId votedFor, NodeId leaderId, ElectionTimeout electionTimeout) {
        super(NodeRole.FOLLOWER, term);
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

    public static boolean isStableBetween(FollowerNodeState before, FollowerNodeState after) {
        return before.term == after.term && before.votedFor == after.votedFor && before.leaderId == after.leaderId;
    }

    @Override
    public NodeStateSnapshot takeSnapshot() {
        NodeStateSnapshot snapshot = new NodeStateSnapshot(this.role, this.term);
        snapshot.setVotedFor(this.votedFor);
        snapshot.setLeaderId(this.leaderId);
        return snapshot;
    }

    @Override
    protected void cancelTimeoutOrTask() {
        this.electionTimeout.cancel();
    }

    @Override
    protected RequestVoteResult processRequestVoteRpc(NodeStateContext context, RequestVoteRpc rpc) {
        assert rpc.getTerm() == this.term;
        assert rpc.getCandidateId() != null;

        if ((this.votedFor == null && !context.getLog().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm())) ||
                Objects.equals(this.votedFor, rpc.getCandidateId())) {

            // vote for candidate
            context.changeToNodeState(new FollowerNodeState(rpc.getTerm(), rpc.getCandidateId(), null, electionTimeout.reset()));
            return new RequestVoteResult(rpc.getTerm(), true);
        }

        // 1. voted for other peer
        // 2. candidate's log is not up-to-date
        return new RequestVoteResult(rpc.getTerm(), false);
    }

    @Override
    protected AppendEntriesResult processAppendEntriesRpc(NodeStateContext context, AppendEntriesRpc rpc) {
        assert rpc.getTerm() == this.term;

        context.changeToNodeState(new FollowerNodeState(this.term, this.votedFor, rpc.getLeaderId(), electionTimeout.reset()));
        return new AppendEntriesResult(this.term, context.getLog().appendEntries(rpc));
    }

    @Override
    public String toString() {
        return "FollowerNodeState{" +
                electionTimeout +
                ", leaderId=" + leaderId +
                ", term=" + term +
                ", votedFor=" + votedFor +
                '}';
    }

}
