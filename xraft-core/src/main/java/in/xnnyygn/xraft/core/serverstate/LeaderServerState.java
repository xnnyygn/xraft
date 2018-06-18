package in.xnnyygn.xraft.core.serverstate;

import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import in.xnnyygn.xraft.core.rpc.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderServerState extends AbstractServerState {

    private static final Logger logger = LoggerFactory.getLogger(LeaderServerState.class);
    private final LogReplicationTask logReplicationTask;

    public LeaderServerState(int term, LogReplicationTask logReplicationTask) {
        super(ServerRole.LEADER, term);
        this.logReplicationTask = logReplicationTask;
    }

    @Override
    public ServerStateSnapshot takeSnapshot() {
        return new ServerStateSnapshot(this.role, this.term);
    }

    @Override
    protected void cancelTimeoutOrTask() {
        this.logReplicationTask.cancel();
    }

    @Override
    public void onReceiveRequestVoteResult(ServerStateContext context, RequestVoteResult result) {
        logger.debug("Server {}, current role is LEADER, ignore", context.getSelfServerId());
    }

    @Override
    protected RequestVoteResult processRequestVoteRpc(ServerStateContext context, RequestVoteRpc rpc) {
        logger.debug("Server {}, current role is LEADER, ignore", context.getSelfServerId());
        return new RequestVoteResult(this.term, false);
    }

    @Override
    protected AppendEntriesResult processAppendEntriesRpc(ServerStateContext context, AppendEntriesRpc rpc) {
        logger.warn("Server {}, receive AppendEntries RPC from another leader, source {}", context.getSelfServerId(), rpc.getLeaderId());
        return new AppendEntriesResult(this.term, false);
    }

    @Override
    public String toString() {
        return "LeaderServerState{" +
                logReplicationTask +
                ", term=" + term +
                '}';
    }

}
