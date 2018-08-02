package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.rpc.message.*;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CandidateNodeRole extends AbstractNodeRole {

    private static final Logger logger = LoggerFactory.getLogger(CandidateNodeRole.class);
    private final int votesCount;
    private final ElectionTimeout electionTimeout;

    CandidateNodeRole(int term, ElectionTimeout electionTimeout) {
        this(term, 1, electionTimeout);
    }

    CandidateNodeRole(int term, int votesCount, ElectionTimeout electionTimeout) {
        super(RoleName.CANDIDATE, term);
        this.votesCount = votesCount;
        this.electionTimeout = electionTimeout;
    }

    @Override
    public RoleStateSnapshot takeSnapshot() {
        RoleStateSnapshot snapshot = new RoleStateSnapshot(this.role, this.term);
        snapshot.setVotesCount(this.votesCount);
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
            int currentVotesCount = this.votesCount + 1;
            int countOfMajor = context.getNodeGroup().getCountOfMajor();
            logger.debug("votes count {}, major node count {}", currentVotesCount, countOfMajor);
            if (currentVotesCount > countOfMajor / 2) {
                electionTimeout.cancel();
                context.resetReplicationStates();
                context.changeToNodeRole(new LeaderNodeRole(this.term, context.scheduleLogReplicationTask()));
                context.getLog().appendEntry(this.term); // no-op log
                context.getConnector().resetChannels();
            } else {
                context.changeToNodeRole(new CandidateNodeRole(this.term, currentVotesCount, electionTimeout.reset()));
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
        return new AppendEntriesResult(rpc.getMessageId(), this.term, context.getLog().appendEntries(rpc));
    }

    @Override
    public String toString() {
        return "CandidateNodeRole{" +
                electionTimeout +
                ", term=" + term +
                ", votesCount=" + votesCount +
                '}';
    }

}
