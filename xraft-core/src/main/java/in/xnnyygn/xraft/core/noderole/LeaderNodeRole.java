package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.replication.NewNodeReplicationState;
import in.xnnyygn.xraft.core.log.replication.ReplicationState;
import in.xnnyygn.xraft.core.log.snapshot.EntryInSnapshotException;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderNodeRole extends AbstractNodeRole {

    private static final Logger logger = LoggerFactory.getLogger(LeaderNodeRole.class);
    private final LogReplicationTask logReplicationTask;

    public LeaderNodeRole(int term, LogReplicationTask logReplicationTask) {
        super(RoleName.LEADER, term);
        this.logReplicationTask = logReplicationTask;
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
        if (context.getNodeGroup().isUniqueNode(context.getSelfNodeId())) {
            context.getLog().advanceCommitIndex(context.getLog().getNextIndex() - 1, this.term);
        } else {
            for (ReplicationState replicationState : context.getNodeGroup().getReplicationTargets()) {
                if (replicationState.isReplicating() &&
                        System.currentTimeMillis() - replicationState.getLastReplicatedAt() <= 900L) { // 0.9s
                    logger.debug("node {} is replicating, skip replication task", replicationState.getNodeId());
                } else {
                    doReplicateLog(context, replicationState, 10);
                }
            }
        }
    }

    public void replicateLog(NodeRoleContext context, NodeId id) {
        ReplicationState replicationState = context.getNodeGroup().getReplicationState(id);
        doReplicateLog(context, replicationState, 20);
    }

    private void doReplicateLog(NodeRoleContext context, ReplicationState replicationState, int maxEntries) {
        replicationState.setReplicating(true);
        replicationState.setLastReplicatedAt(System.currentTimeMillis());
        try {
            AppendEntriesRpc rpc = context.getLog().createAppendEntriesRpc(this.term, context.getSelfNodeId(), replicationState.getNextIndex(), maxEntries);
            context.getConnector().sendAppendEntries(rpc, replicationState.getNodeId());
        } catch (EntryInSnapshotException e) {
            logger.debug("log entry {} in snapshot, replicate with InstallSnapshot RPC", replicationState.getNextIndex());
            InstallSnapshotRpc rpc = context.getLog().createInstallSnapshotRpc(this.term, context.getSelfNodeId(), 0);
            context.getConnector().sendInstallSnapshot(rpc, replicationState.getNodeId());
        }
    }

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
        return new AppendEntriesResult(rpc.getMessageId(), this.term, false);
    }

    @Override
    protected void processAppendEntriesResult(NodeRoleContext context, AppendEntriesResult result, NodeId sourceNodeId, AppendEntriesRpc rpc) {
        assert result.getTerm() <= this.term;

        NodeGroup.NodeState nodeState = context.getNodeGroup().getState(sourceNodeId);
        if (nodeState == null) {
            logger.warn("node {} removed before receiving append entries result", sourceNodeId);
            return;
        }

        ReplicationState replicationState = nodeState.getReplicationState();

        if (result.isSuccess()) {
            if (nodeState.isMemberOfMajor()) {

                // peer
                if (replicationState.advance(rpc.getLastEntryIndex())) {
                    context.getLog().advanceCommitIndex(context.getNodeGroup().getMatchIndexOfMajor(), this.term);
                }

                if (replicationState.catchUp(context.getLog().getNextIndex())) {
                    replicationState.setReplicating(false);
                    return;
                }
            } else {
                // skip removing node
                if (nodeState.isRemoving()) {
                    replicationState.setReplicating(false);
                    return;
                }

                // new node
                logger.debug("replication state of new node, {}", replicationState);

                replicationState.advance(rpc.getLastEntryIndex());
                if (replicationState.catchUp(context.getLog().getNextIndex())) {
                    context.upgradeNode(sourceNodeId);
                    replicationState.setReplicating(false);
                    return;
                }

                NewNodeReplicationState newNodeReplicationState = (NewNodeReplicationState) replicationState;
                newNodeReplicationState.increaseRound();
                if (newNodeReplicationState.roundExceedOrTimeout(300L)) {
                    // replication state will also be removed
                    context.removeNode(sourceNodeId);
                    return;
                }
            }
        } else {
            if (!replicationState.backOffNextIndex()) {
                logger.warn("cannot back off next index more, node {}", sourceNodeId);
                replicationState.setReplicating(false);
                return;
            }
        }

        doReplicateLog(context, replicationState, ALL_ENTRIES);
    }

    @Override
    protected void processInstallSnapshotResult(NodeRoleContext context, InstallSnapshotResult result, NodeId sourceNodeId, InstallSnapshotRpc rpc) {
        if (rpc.isDone()) {
            ReplicationState replicationState = context.getNodeGroup().getReplicationState(sourceNodeId);
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
