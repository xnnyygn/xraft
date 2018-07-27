package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.replication.GeneralReplicationStateTracker;
import in.xnnyygn.xraft.core.log.replication.ReplicationState;
import in.xnnyygn.xraft.core.log.replication.SelfReplicationState;
import in.xnnyygn.xraft.core.log.snapshot.EntryInSnapshotException;
import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class LeaderNodeRole extends AbstractNodeRole {

    private static final Logger logger = LoggerFactory.getLogger(LeaderNodeRole.class);
    private final LogReplicationTask logReplicationTask;
    private final GeneralReplicationStateTracker replicationStateTracker;

    public LeaderNodeRole(int term, LogReplicationTask logReplicationTask, GeneralReplicationStateTracker replicationStateTracker) {
        super(RoleName.LEADER, term);
        this.logReplicationTask = logReplicationTask;
        this.replicationStateTracker = replicationStateTracker;
    }

    @Override
    public RoleStateSnapshot takeSnapshot() {
        return new RoleStateSnapshot(this.role, this.term);
    }

    @Override
    public void cancelTimeoutOrTask() {
        this.logReplicationTask.cancel();
    }

    public void replicateLog(NodeRoleContext context) {
        for (NodeId nodeId : this.replicationStateTracker.listPeer()) {
            this.doReplicateLog(context, nodeId, -1);
        }
    }

    public void replicateLog(NodeRoleContext context, NodeId nodeId) {
        this.doReplicateLog(context, nodeId, -1);
    }

    private void doReplicateLog(NodeRoleContext context, NodeId nodeId, int maxEntries) {
        ReplicationState replicationState = this.replicationStateTracker.get(nodeId);
        try {
            AppendEntriesRpc rpc = context.getLog().createAppendEntriesRpc(this.term, context.getSelfNodeId(), replicationState.getNextIndex(), maxEntries);
            context.getConnector().sendAppendEntries(rpc, nodeId);
        } catch (EntryInSnapshotException e) {
            logger.debug("log entry {} in snapshot, replicate with InstallSnapshot RPC", replicationState.getNextIndex());
            InstallSnapshotRpc rpc = context.getLog().createInstallSnapshotRpc(this.term, context.getSelfNodeId(), 0);
            context.getConnector().sendInstallSnapshot(rpc, nodeId);
        }
    }

    public void addServer(NodeConfig newNodeConfig) {

    }

    public void removeServer(NodeConfig nodeConfig) {

    }

//    public void applyNodeConfigs(NodeRoleContext context, Set<NodeConfig> nodeConfigs) {
//        context.getLog().appendEntry(this.term, nodeConfigs);
//        this.replicateLog(context);
//        // TODO apply to node group
//        context.getConnector().applyNodeConfigs(nodeConfigs, context.getSelfNodeId());
//        this.replicationStateTracker.applyNodeConfigs(nodeConfigs, context.getLog().getNextLogIndex(),
//                context.getSelfNodeId(), new SelfReplicationState(context.getSelfNodeId(), context.getLog()));
//    }

    @Override
    protected RequestVoteResult processRequestVoteRpc(NodeRoleContext context, RequestVoteRpc rpc) {
        assert rpc.getTerm() == this.term;

        logger.debug("Node {}, current role is LEADER, ignore request vote rpc", context.getSelfNodeId());
        return new RequestVoteResult(this.term, false);
    }

    @Override
    protected AppendEntriesResult processAppendEntriesRpc(NodeRoleContext context, AppendEntriesRpc rpc) {
        assert rpc.getTerm() == this.term;

        logger.warn("Node {}, ignore AppendEntries RPC from another leader, node {}", context.getSelfNodeId(), rpc.getLeaderId());
        return new AppendEntriesResult(this.term, false);
    }

    @Override
    protected void processAppendEntriesResult(NodeRoleContext context, AppendEntriesResult result, NodeId sourceNodeId, AppendEntriesRpc rpc) {
        assert result.getTerm() <= this.term;

        ReplicationState replicationState = this.replicationStateTracker.get(sourceNodeId);
        if (result.isSuccess()) {
            if (replicationState.advance(rpc.getLastEntryIndex())) {
                context.getLog().advanceCommitIndex(this.replicationStateTracker.getMajorMatchIndex(), this.term);
            }
            if (replicationState.getNextIndex() >= context.getLog().getNextLogIndex()) {
                return;
            }
        } else {
            replicationState.backOffNextIndex();
        }
        this.doReplicateLog(context, sourceNodeId, ALL_ENTRIES);
    }

    @Override
    protected void processInstallSnapshotResult(NodeRoleContext context, InstallSnapshotResult result, NodeId sourceNodeId, InstallSnapshotRpc rpc) {
        if (rpc.isDone()) {
            ReplicationState replicationState = this.replicationStateTracker.get(sourceNodeId);
            replicationState.advance(rpc.getLastIncludedIndex());
            return;
        }

        InstallSnapshotRpc nextRpc = context.getLog().createInstallSnapshotRpc(this.term, context.getSelfNodeId(), rpc.getOffset() + rpc.getDataLength());
        context.getConnector().sendInstallSnapshot(nextRpc, sourceNodeId);
    }

    @Override
    public String toString() {
        return "LeaderNodeRole{" +
                logReplicationTask +
                ", term=" + term +
                '}';
    }

}
