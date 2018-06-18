package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.rpc.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CandidateNodeState extends AbstractNodeState {

    private static final Logger logger = LoggerFactory.getLogger(CandidateNodeState.class);
    private final int votedCount;
    private final ElectionTimeout electionTimeout;

    public CandidateNodeState(int term, ElectionTimeout electionTimeout) {
        this(term, 1, electionTimeout);
    }

    public CandidateNodeState(int term, int votedCount, ElectionTimeout electionTimeout) {
        super(NodeRole.CANDIDATE, term);
        this.votedCount = votedCount;
        this.electionTimeout = electionTimeout;
    }

    @Override
    public NodeStateSnapshot takeSnapshot() {
        NodeStateSnapshot snapshot = new NodeStateSnapshot(this.role, this.term);
        snapshot.setVotesCount(this.votedCount);
        return snapshot;
    }

    @Override
    protected void cancelTimeoutOrTask() {
        this.electionTimeout.cancel();
    }

    @Override
    public void onReceiveRequestVoteResult(NodeStateContext context, RequestVoteResult result) {
        if (result.isVoteGranted()) {
            int votesCount = this.votedCount + 1;
            if (votesCount > (context.getNodeCount() / 2)) {
                this.electionTimeout.cancel();
                context.setNodeState(new LeaderNodeState(this.term, context.scheduleLogReplicationTask()));
            } else {
                context.setNodeState(new CandidateNodeState(this.term, votedCount, electionTimeout.reset()));
            }
        } else if (result.getTerm() > this.term) {
            logger.debug("Node {}, update to peer's term", context.getSelfNodeId(), result.getTerm());

            // current term is old
            context.setNodeState(new FollowerNodeState(result.getTerm(), null, null, electionTimeout.reset()));
        }
    }

    @Override
    protected RequestVoteResult processRequestVoteRpc(NodeStateContext context, RequestVoteRpc rpc) {

        // voted for self
        return new RequestVoteResult(this.term, false);
    }

    @Override
    protected AppendEntriesResult processAppendEntriesRpc(NodeStateContext context, AppendEntriesRpc rpc) {
        // more than 1 candidate but another node win the election
        context.setNodeState(new FollowerNodeState(this.term, null, rpc.getLeaderId(), electionTimeout.reset()));
        return new AppendEntriesResult(this.term, true);
    }

    @Override
    public String toString() {
        return "CandidateNodeState{" +
                electionTimeout +
                ", term=" + term +
                ", votedCount=" + votedCount +
                '}';
    }

}
