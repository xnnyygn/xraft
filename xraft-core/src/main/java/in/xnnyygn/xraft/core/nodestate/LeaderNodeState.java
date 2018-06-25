package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.log.ReplicationState;
import in.xnnyygn.xraft.core.log.ReplicationStateTracker;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.RequestVoteRpc;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderNodeState extends AbstractNodeState {

    private static final Logger logger = LoggerFactory.getLogger(LeaderNodeState.class);
    private final LogReplicationTask logReplicationTask;
    private final ReplicationStateTracker replicationStateTracker;

    public LeaderNodeState(int term, LogReplicationTask logReplicationTask, ReplicationStateTracker replicationStateTracker) {
        super(NodeRole.LEADER, term);
        this.logReplicationTask = logReplicationTask;
        this.replicationStateTracker = replicationStateTracker;
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
    public void replicateLog(NodeStateContext context, int maxEntries) {
        for (NodeId nodeId : this.replicationStateTracker.listNodeNotReplicating()) {
            this.doReplicateLog(context, nodeId, maxEntries);
        }
    }

    private void doReplicateLog(NodeStateContext context, NodeId nodeId, int maxEntries) {
        ReplicationState replicationState = this.replicationStateTracker.get(nodeId);
        AppendEntriesRpc rpc = context.getLog().createAppendEntriesRpc(this.term, context.getSelfNodeId(), replicationState.getNextIndex(), maxEntries);
        context.getConnector().sendRpc(rpc, nodeId);
    }

    @Override
    protected RequestVoteResult processRequestVoteRpc(NodeStateContext context, RequestVoteRpc rpc) {
        assert rpc.getTerm() == this.term;

        logger.debug("Node {}, current role is LEADER, ignore request vote rpc", context.getSelfNodeId());
        return new RequestVoteResult(this.term, false);
    }

    @Override
    protected AppendEntriesResult processAppendEntriesRpc(NodeStateContext context, AppendEntriesRpc rpc) {
        assert rpc.getTerm() == this.term;

        logger.warn("Node {}, ignore AppendEntries RPC from another leader, source {}", context.getSelfNodeId(), rpc.getLeaderId());
        return new AppendEntriesResult(this.term, false);
    }

    @Override
    protected void processAppendEntriesResult(NodeStateContext context, AppendEntriesResult result, NodeId sourceNodeId, AppendEntriesRpc rpc) {
        assert result.getTerm() <= this.term;

        ReplicationState replicationState = this.replicationStateTracker.get(sourceNodeId);
        if (result.isSuccess()) {
            if (rpc.hasEntry() || rpc.getPrevLogIndex() > 0) {
                replicationState.advance(rpc.getLastEntryIndex());
                context.getLog().advanceCommitIndexIfAvailable(this.replicationStateTracker.getMajorMatchIndex());
            }
            // if there's no log to replicate, skip
            if (replicationState.getNextIndex() > context.getLog().getLastLogIndex()) {
                replicationState.setReplicating(false);
                return;
            }
        } else {
            replicationState.backOffNextIndex();
        }
        this.doReplicateLog(context, sourceNodeId, ALL_ENTRIES);
    }

    @Override
    public String toString() {
        return "LeaderNodeState{" +
                logReplicationTask +
                ", term=" + term +
                '}';
    }

}
