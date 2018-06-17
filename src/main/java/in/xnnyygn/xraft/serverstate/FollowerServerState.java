package in.xnnyygn.xraft.serverstate;

import in.xnnyygn.xraft.schedule.ElectionTimeout;
import in.xnnyygn.xraft.server.ServerId;
import in.xnnyygn.xraft.server.ServerStore;
import in.xnnyygn.xraft.rpc.AppendEntriesResult;
import in.xnnyygn.xraft.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.rpc.RequestVoteResult;
import in.xnnyygn.xraft.rpc.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowerServerState extends AbstractServerState {

    private static final Logger logger = LoggerFactory.getLogger(FollowerServerState.class);
    private final ServerId votedFor;
    private final ServerId leaderId;
    private final ElectionTimeout electionTimeout;

    public FollowerServerState(ServerStore serverStore, ElectionTimeout electionTimeout) {
        this(serverStore.getCurrentTerm(), serverStore.getVotedFor(), null, electionTimeout);
    }

    public FollowerServerState(int term, ServerId votedFor, ServerId leaderId, ElectionTimeout electionTimeout) {
        super(ServerRole.FOLLOWER, term);
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

    public static boolean isStableBetween(FollowerServerState before, FollowerServerState after) {
        return before.term == after.term && before.votedFor == after.votedFor && before.leaderId == after.leaderId;
    }

    @Override
    public ServerStateSnapshot takeSnapshot() {
        ServerStateSnapshot snapshot = new ServerStateSnapshot(this.role, this.term);
        snapshot.setVotedFor(this.votedFor);
        snapshot.setLeaderId(this.leaderId);
        return snapshot;
    }

    @Override
    protected void cancelTimeoutOrTask() {
        this.electionTimeout.cancel();
    }

    @Override
    public void onReceiveRequestVoteResult(ServerStateContext context, RequestVoteResult result) {
        logger.warn("Server {}, current role is FOLLOWER, ignore", context.getSelfServerId());
    }

    @Override
    protected RequestVoteResult processRequestVoteRpc(ServerStateContext context, RequestVoteRpc rpc) {
        if (this.votedFor == null || this.votedFor.equals(rpc.getCandidateId())) {

            // vote for candidate
            context.setServerState(new FollowerServerState(this.term, rpc.getCandidateId(), null, electionTimeout.reset()));
            return new RequestVoteResult(this.term, true);
        }

        // voted for other peer
        return new RequestVoteResult(this.term, false);
    }

    @Override
    protected AppendEntriesResult processAppendEntriesRpc(ServerStateContext context, AppendEntriesRpc rpc) {
        context.setServerState(new FollowerServerState(this.term, this.votedFor, rpc.getLeaderId(), electionTimeout.reset()));
        return new AppendEntriesResult(this.term, true);
    }

    @Override
    public String toString() {
        return "FollowerServerState{" +
                electionTimeout +
                ", leaderId=" + leaderId +
                ", term=" + term +
                ", votedFor=" + votedFor +
                '}';
    }

}
