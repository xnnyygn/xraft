package in.xnnyygn.xraft.nodestate;

import in.xnnyygn.xraft.scheduler.ElectionTimeout;
import in.xnnyygn.xraft.node.RaftNodeId;
import in.xnnyygn.xraft.node.RaftNodeSave;
import in.xnnyygn.xraft.rpc.AppendEntriesResult;
import in.xnnyygn.xraft.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.rpc.RequestVoteResult;
import in.xnnyygn.xraft.rpc.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowerNodeState extends AbstractNodeState {

    private static final Logger logger = LoggerFactory.getLogger(FollowerNodeState.class);
    private final RaftNodeId votedFor;
    private final RaftNodeId leaderId;
    private final ElectionTimeout electionTimeout;

    public FollowerNodeState(RaftNodeSave nodeSave, ElectionTimeout electionTimeout) {
        this(nodeSave.getCurrentTerm(), nodeSave.getVotedFor(), null, electionTimeout);
    }

    public FollowerNodeState(int term, RaftNodeId votedFor, RaftNodeId leaderId, ElectionTimeout electionTimeout) {
        super(NodeRole.FOLLOWER, term);
        this.votedFor = votedFor;
        this.leaderId = leaderId;
        this.electionTimeout = electionTimeout;
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
        logger.warn("Node {}, current role is FOLLOWER, ignore", context.getSelfNodeId());
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
