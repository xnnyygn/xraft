package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.schedule.ElectionTimeout;

import javax.annotation.concurrent.Immutable;

@Immutable
public class CandidateNodeRole extends AbstractNodeRole {

    private final int votesCount;
    private final ElectionTimeout electionTimeout;

    public CandidateNodeRole(int term, ElectionTimeout electionTimeout) {
        this(term, 1, electionTimeout);
    }

    public CandidateNodeRole(int term, int votesCount, ElectionTimeout electionTimeout) {
        super(RoleName.CANDIDATE, term);
        this.votesCount = votesCount;
        this.electionTimeout = electionTimeout;
    }

    public int getVotesCount() {
        return votesCount;
    }

    @Override
    public NodeId getLeaderId(NodeId selfId) {
        return null;
    }

    @Override
    public void cancelTimeoutOrTask() {
        electionTimeout.cancel();
    }

    @Override
    public RoleState getState() {
        RoleState state = new RoleState(RoleName.CANDIDATE, term);
        state.setVotesCount(votesCount);
        return state;
    }

    @Override
    public String toString() {
        return "CandidateNodeRole{" +
                "term=" + term +
                ", votesCount=" + votesCount +
                ", electionTimeout=" + electionTimeout +
                '}';
    }
}
