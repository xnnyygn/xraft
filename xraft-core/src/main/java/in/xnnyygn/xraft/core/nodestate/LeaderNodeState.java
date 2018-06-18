package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import in.xnnyygn.xraft.core.rpc.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderNodeState extends AbstractNodeState {

    private static final Logger logger = LoggerFactory.getLogger(LeaderNodeState.class);
    private final LogReplicationTask logReplicationTask;

    public LeaderNodeState(int term, LogReplicationTask logReplicationTask) {
        super(NodeRole.LEADER, term);
        this.logReplicationTask = logReplicationTask;
    }

    @Override
    public NodeStateSnapshot takeSnapshot() {
        return new NodeStateSnapshot(this.role, this.term);
    }

    @Override
    protected void cancelTimeoutOrTask() {
        this.logReplicationTask.cancel();
    }

    @Override
    public void onReceiveRequestVoteResult(NodeStateContext context, RequestVoteResult result) {
        logger.debug("Server {}, current role is LEADER, ignore", context.getSelfNodeId());
    }

    @Override
    protected RequestVoteResult processRequestVoteRpc(NodeStateContext context, RequestVoteRpc rpc) {
        logger.debug("Server {}, current role is LEADER, ignore", context.getSelfNodeId());
        return new RequestVoteResult(this.term, false);
    }

    @Override
    protected AppendEntriesResult processAppendEntriesRpc(NodeStateContext context, AppendEntriesRpc rpc) {
        logger.warn("Server {}, receive AppendEntries RPC from another leader, source {}", context.getSelfNodeId(), rpc.getLeaderId());
        return new AppendEntriesResult(this.term, false);
    }

    @Override
    public String toString() {
        return "LeaderNodeState{" +
                logReplicationTask +
                ", term=" + term +
                '}';
    }

}
