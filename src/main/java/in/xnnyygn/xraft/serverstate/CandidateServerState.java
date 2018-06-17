package in.xnnyygn.xraft.serverstate;

import in.xnnyygn.xraft.scheduler.ElectionTimeout;
import in.xnnyygn.xraft.rpc.AppendEntriesResult;
import in.xnnyygn.xraft.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.rpc.RequestVoteResult;
import in.xnnyygn.xraft.rpc.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CandidateServerState extends AbstractServerState {

    private static final Logger logger = LoggerFactory.getLogger(CandidateServerState.class);
    private final int votedCount;
    private final ElectionTimeout electionTimeout;

    public CandidateServerState(int term, ElectionTimeout electionTimeout) {
        this(term, 1, electionTimeout);
    }

    public CandidateServerState(int term, int votedCount, ElectionTimeout electionTimeout) {
        super(NodeRole.CANDIDATE, term);
        this.votedCount = votedCount;
        this.electionTimeout = electionTimeout;
    }

    @Override
    public ServerStateSnapshot takeSnapshot() {
        ServerStateSnapshot snapshot = new ServerStateSnapshot(this.role, this.term);
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
                context.setNodeState(new LeaderServerState(this.term, context.scheduleLogReplicationTask()));
            } else {
                context.setNodeState(new CandidateServerState(this.term, votedCount, electionTimeout.reset()));
            }
        } else if (result.getTerm() > this.term) {
            logger.debug("Node {}, update to peer's term", context.getSelfNodeId(), result.getTerm());

            // current term is old
            context.setNodeState(new FollowerServerState(result.getTerm(), null, null, electionTimeout.reset()));
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
        context.setNodeState(new FollowerServerState(this.term, null, rpc.getLeaderId(), electionTimeout.reset()));
        return new AppendEntriesResult(this.term, true);
    }

    @Override
    public String toString() {
        return "CandidateServerState{" +
                electionTimeout +
                ", term=" + term +
                ", votedCount=" + votedCount +
                '}';
    }

}
