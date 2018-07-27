package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.rpc.message.*;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;

public class CandidateNodeRole extends AbstractNodeRole {

    private final int votedCount;
    private final ElectionTimeout electionTimeout;

    CandidateNodeRole(int term, ElectionTimeout electionTimeout) {
        this(term, 1, electionTimeout);
    }

    CandidateNodeRole(int term, int votedCount, ElectionTimeout electionTimeout) {
        super(RoleName.CANDIDATE, term);
        this.votedCount = votedCount;
        this.electionTimeout = electionTimeout;
    }

    @Override
    public RoleStateSnapshot takeSnapshot() {
        RoleStateSnapshot snapshot = new RoleStateSnapshot(this.role, this.term);
        snapshot.setVotesCount(this.votedCount);
        return snapshot;
    }

    @Override
    public void cancelTimeoutOrTask() {
        this.electionTimeout.cancel();
    }

    @Override
    protected void processRequestVoteResult(NodeRoleContext context, RequestVoteResult result) {
        assert result.getTerm() <= this.term;

        if (result.isVoteGranted()) {
            int votesCount = this.votedCount + 1;
            if (votesCount > (context.getNodeCountForVoting() / 2)) {
                this.electionTimeout.cancel();
                context.changeToNodeRole(new LeaderNodeRole(this.term, context.scheduleLogReplicationTask(), context.createReplicationStateTracker()));
                context.getLog().appendEntry(this.term); // no-op entry
                context.getConnector().resetChannels();
            } else {
                context.changeToNodeRole(new CandidateNodeRole(this.term, votedCount, electionTimeout.reset()));
            }
        }
    }

    @Override
    protected RequestVoteResult processRequestVoteRpc(NodeRoleContext context, RequestVoteRpc rpc) {
        assert rpc.getTerm() == this.term;

        // voted for self
        return new RequestVoteResult(this.term, false);
    }

    @Override
    protected AppendEntriesResult processAppendEntriesRpc(NodeRoleContext context, AppendEntriesRpc rpc) {
        assert rpc.getTerm() == this.term;

        // more than 1 candidate but another node win the election
        context.changeToNodeRole(new FollowerNodeRole(this.term, null, rpc.getLeaderId(), electionTimeout.reset()));
        return new AppendEntriesResult(this.term, context.getLog().appendEntries(rpc));
    }

    @Override
    public String toString() {
        return "CandidateNodeRole{" +
                electionTimeout +
                ", term=" + term +
                ", votedCount=" + votedCount +
                '}';
    }

}
