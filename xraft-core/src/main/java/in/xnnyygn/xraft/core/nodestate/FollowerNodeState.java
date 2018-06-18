package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.server.ServerId;
import in.xnnyygn.xraft.core.server.ServerStore;
import in.xnnyygn.xraft.core.rpc.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowerNodeState extends AbstractNodeState {

    private static final Logger logger = LoggerFactory.getLogger(FollowerNodeState.class);
    private final ServerId votedFor;
    private final ServerId leaderId;
    private final ElectionTimeout electionTimeout;

    public FollowerNodeState(ServerStore serverStore, ElectionTimeout electionTimeout) {
        this(serverStore.getCurrentTerm(), serverStore.getVotedFor(), null, electionTimeout);
    }

    public FollowerNodeState(int term, ServerId votedFor, ServerId leaderId, ElectionTimeout electionTimeout) {
        super(NodeRole.FOLLOWER, term);
        this.votedFor = votedFor;
        this.leaderId = leaderId;
        this.electionTimeout = electionTimeout;
    }

    public ServerId getVotedFor() {
        return votedFor;
    }

    public ServerId getLeaderId() {
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
    public void onReceiveRequestVoteResult(NodeStateContext context, RequestVoteResult result) {
        logger.warn("Server {}, current role is FOLLOWER, ignore", context.getSelfNodeId());
    }

    @Override
    protected RequestVoteResult processRequestVoteRpc(NodeStateContext context, RequestVoteRpc rpc) {
        if (this.votedFor == null || this.votedFor.equals(rpc.getCandidateId())) {

            // vote for candidate
            context.setNodeState(new FollowerNodeState(this.term, rpc.getCandidateId(), null, electionTimeout.reset()));
            return new RequestVoteResult(this.term, true);
        }

        // voted for other peer
        return new RequestVoteResult(this.term, false);
    }

    @Override
    protected AppendEntriesResult processAppendEntriesRpc(NodeStateContext context, AppendEntriesRpc rpc) {
        context.setNodeState(new FollowerNodeState(this.term, this.votedFor, rpc.getLeaderId(), electionTimeout.reset()));
        return new AppendEntriesResult(this.term, true);
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
