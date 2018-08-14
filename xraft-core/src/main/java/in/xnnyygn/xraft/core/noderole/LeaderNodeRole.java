package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.log.replication.NewNodeReplicatingState;
import in.xnnyygn.xraft.core.log.replication.ReplicatingState;
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
            for (ReplicatingState replicatingState : context.getNodeGroup().getReplicationTargets()) {
                if (replicatingState.isReplicating() &&
                        System.currentTimeMillis() - replicatingState.getLastReplicatedAt() <= 900L) { // 0.9s
                    logger.debug("node {} is replicating, skip replication task", replicatingState.getNodeId());
                } else {
                    doReplicateLog(context, replicatingState, 10);
                }
            }
        }
    }

    public void replicateLog(NodeRoleContext context, NodeId id) {
        ReplicatingState replicatingState = context.getNodeGroup().findReplicationState(id);
        doReplicateLog(context, replicatingState, 20);
    }

    private void doReplicateLog(NodeRoleContext context, ReplicatingState replicatingState, int maxEntries) {
        replicatingState.startReplicating();
        try {
            AppendEntriesRpc rpc = context.getLog().createAppendEntriesRpc(this.term, context.getSelfNodeId(), replicatingState.getNextIndex(), maxEntries);
            context.getConnector().sendAppendEntries(rpc, replicatingState.getNodeId());
        } catch (EntryInSnapshotException e) {
            logger.debug("log entry {} in snapshot, replicate with InstallSnapshot RPC", replicatingState.getNextIndex());
            InstallSnapshotRpc rpc = context.getLog().createInstallSnapshotRpc(this.term, context.getSelfNodeId(), 0, 1024);
            context.getConnector().sendInstallSnapshot(rpc, replicatingState.getNodeId());
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

        ReplicatingState replicatingState = nodeState.getReplicatingState();

        if (result.isSuccess()) {
            if (nodeState.isMemberOfMajor()) {

                // peer
                if (replicatingState.advance(rpc.getLastEntryIndex())) {
                    context.getLog().advanceCommitIndex(context.getNodeGroup().getMatchIndexOfMajor(), this.term);
                }

                if (replicatingState.catchUp(context.getLog().getNextIndex())) {
                    replicatingState.stopReplicating();
                    return;
                }
            } else {
                // skip removing node
                if (nodeState.isRemoving()) {
                    replicatingState.stopReplicating();
                    return;
                }

                // new node
                logger.debug("replication state of new node, {}", replicatingState);

                replicatingState.advance(rpc.getLastEntryIndex());
                if (replicatingState.catchUp(context.getLog().getNextIndex())) {
                    context.upgradeNode(sourceNodeId);
                    replicatingState.stopReplicating();
                    return;
                }

                NewNodeReplicatingState newNodeReplicationState = (NewNodeReplicatingState) replicatingState;
                newNodeReplicationState.increaseRound();
                if (newNodeReplicationState.roundExceedOrTimeout(10, 300L)) {
                    // replication state will also be removed
                    context.removeNode(sourceNodeId);
                    return;
                }
            }
        } else {
            if (!replicatingState.backOffNextIndex()) {
                logger.warn("cannot back off next index more, node {}", sourceNodeId);
                replicatingState.stopReplicating();
                return;
            }
        }

        doReplicateLog(context, replicatingState, ALL_ENTRIES);
    }

    @Override
    protected void processInstallSnapshotResult(NodeRoleContext context, InstallSnapshotResult result, NodeId sourceNodeId, InstallSnapshotRpc rpc) {
        if (rpc.isDone()) {
            ReplicatingState replicatingState = context.getNodeGroup().findReplicationState(sourceNodeId);
            replicatingState.advance(rpc.getLastIncludedIndex());
            return;
        }

        InstallSnapshotRpc nextRpc = context.getLog().createInstallSnapshotRpc(this.term, context.getSelfNodeId(), rpc.getOffset() + rpc.getDataLength(), 1024);
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
