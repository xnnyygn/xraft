package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpc;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;

public class CandidateNodeState extends AbstractNodeState {

    private final int votedCount;
    private final ElectionTimeout electionTimeout;

    CandidateNodeState(int term, ElectionTimeout electionTimeout) {
        this(term, 1, electionTimeout);
    }

    CandidateNodeState(int term, int votedCount, ElectionTimeout electionTimeout) {
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
    protected void processRequestVoteResult(NodeStateContext context, RequestVoteResult result) {
        assert result.getTerm() <= this.term;

        if (result.isVoteGranted()) {
            int votesCount = this.votedCount + 1;
            if (votesCount > (context.getNodeCount() / 2)) {
                this.electionTimeout.cancel();
                context.changeToNodeState(new LeaderNodeState(this.term, context.scheduleLogReplicationTask(), context.createReplicationStateTracker()));
                context.getLog().appendEntry(this.term); // no-op entry
                context.getConnector().resetChannels();
            } else {
                context.changeToNodeState(new CandidateNodeState(this.term, votedCount, electionTimeout.reset()));
            }
        }
    }

    @Override
    protected RequestVoteResult processRequestVoteRpc(NodeStateContext context, RequestVoteRpc rpc) {
        assert rpc.getTerm() == this.term;

        // voted for self
        return new RequestVoteResult(this.term, false);
    }

    @Override
    protected AppendEntriesResult processAppendEntriesRpc(NodeStateContext context, AppendEntriesRpc rpc) {
        assert rpc.getTerm() == this.term;

        // more than 1 candidate but another node win the election
        context.changeToNodeState(new FollowerNodeState(this.term, null, rpc.getLeaderId(), electionTimeout.reset()));
        return new AppendEntriesResult(this.term, context.getLog().appendEntries(rpc));
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
