package in.xnnyygn.xraft.core.noderole;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract node role.
 */
public abstract class AbstractNodeRole {

    public static final int ALL_ENTRIES = -1;

    private static final Logger logger = LoggerFactory.getLogger(AbstractNodeRole.class);
    protected final RoleName role;
    protected final int term;

    /**
     * Create.
     *
     * @param role role
     * @param term term
     */
    AbstractNodeRole(RoleName role, int term) {
        this.role = role;
        this.term = term;
    }

    /**
     * Get role.
     *
     * @return role
     */
    public RoleName getRole() {
        return role;
    }

    public abstract RoleStateSnapshot takeSnapshot();

    /**
     * Get term.
     *
     * @return term
     */
    public int getTerm() {
        return term;
    }

    /**
     * Cancel timeout or task of current state.
     */
    public abstract void cancelTimeoutOrTask();

    /**
     * Called when election timeout.
     *
     * @param context context
     */
    public void electionTimeout(NodeRoleContext context) {
        if (this.role == RoleName.LEADER) {
            logger.warn("node {}, current role is LEADER, ignore election timeout", context.getSelfNodeId());
            return;
        }

        // follower: start election
        // candidate: restart election
        int newTerm = this.term + 1;
        cancelTimeoutOrTask();

        if (context.getNodeGroup().isUniqueNode(context.getSelfNodeId())) {
            if (context.standbyMode()) {
                logger.info("standby mode, skip election");
            } else {
                logger.info("no other node, just become LEADER");
                context.changeToNodeRole(new LeaderNodeRole(newTerm, context.scheduleLogReplicationTask()));
                context.getLog().appendEntry(newTerm); // no-op log
            }
        } else {
            context.changeToNodeRole(new CandidateNodeRole(newTerm, context.scheduleElectionTimeout()));
            RequestVoteRpc rpc = context.getLog().createRequestVoteRpc(newTerm, context.getSelfNodeId());
            context.getConnector().sendRequestVote(rpc);
        }
    }

    /**
     * Called when receive request vote rpc.
     *
     * @param context    context
     * @param rpcMessage rpc message
     */
    public void onReceiveRequestVoteRpc(NodeRoleContext context, RequestVoteRpcMessage rpcMessage) {
        RequestVoteRpc rpc = rpcMessage.get();
        RequestVoteResult result;

        if (rpc.getTerm() > this.term) {
            boolean voteForCandidate = !context.getLog().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm());
            this.cancelTimeoutOrTask();
            context.changeToNodeRole(new FollowerNodeRole(rpc.getTerm(), (voteForCandidate ? rpc.getCandidateId() : null), null, context.scheduleElectionTimeout()));
            result = new RequestVoteResult(rpc.getTerm(), voteForCandidate);
        } else if (rpc.getTerm() == this.term) {
            result = processRequestVoteRpc(context, rpc);
        } else {
            result = new RequestVoteResult(this.term, false);
        }

        context.getConnector().replyRequestVote(result, rpcMessage);
    }

    /**
     * Process request vote rpc in same term.
     *
     * @param context context
     * @param rpc     rpc
     * @return request vote result
     */
    protected abstract RequestVoteResult processRequestVoteRpc(NodeRoleContext context, RequestVoteRpc rpc);

    /**
     * Called when receive request vote result.
     *
     * @param context context
     * @param result  result
     */
    public void onReceiveRequestVoteResult(NodeRoleContext context, RequestVoteResult result) {
        if (result.getTerm() > this.term) {
            this.cancelTimeoutOrTask();
            context.changeToNodeRole(new FollowerNodeRole(result.getTerm(), null, null, context.scheduleElectionTimeout()));
            return;
        }

        processRequestVoteResult(context, result);
    }

    protected void processRequestVoteResult(NodeRoleContext context, RequestVoteResult result) {
    }

    /**
     * Called when receive append entries rpc.
     *
     * @param context    context
     * @param rpcMessage rpc message
     */
    public void onReceiveAppendEntriesRpc(NodeRoleContext context, AppendEntriesRpcMessage rpcMessage) {
        AppendEntriesRpc rpc = rpcMessage.get();
        AppendEntriesResult result;

        if (rpc.getTerm() > this.term) {
            this.cancelTimeoutOrTask();
            context.changeToNodeRole(new FollowerNodeRole(rpc.getTerm(), null, rpc.getLeaderId(), context.scheduleElectionTimeout()));
            result = new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), context.getLog().appendEntries(rpc));
        } else if (rpc.getTerm() == this.term) {
            result = processAppendEntriesRpc(context, rpc);
        } else {
            result = new AppendEntriesResult(rpc.getMessageId(), this.term, false);
        }

        context.getConnector().replyAppendEntries(result, rpcMessage);
    }

    /**
     * Process append entries rpc in same term.
     *
     * @param context context
     * @param rpc     rpc
     * @return append entries result
     */
    protected abstract AppendEntriesResult processAppendEntriesRpc(NodeRoleContext context, AppendEntriesRpc rpc);

    public void onReceiveAppendEntriesResult(NodeRoleContext context, AppendEntriesResult result, NodeId sourceNodeId, AppendEntriesRpc rpc) {
        if (result.getTerm() > this.term) {
            this.cancelTimeoutOrTask();
            context.changeToNodeRole(new FollowerNodeRole(result.getTerm(), null, null, context.scheduleElectionTimeout()));
            return;
        }

        processAppendEntriesResult(context, result, sourceNodeId, rpc);
    }

    protected void processAppendEntriesResult(NodeRoleContext context, AppendEntriesResult result, NodeId sourceNodeId, AppendEntriesRpc rpc) {
    }


    public void onReceiveInstallSnapshotRpc(NodeRoleContext context, InstallSnapshotRpcMessage rpcMessage) {
        InstallSnapshotRpc rpc = rpcMessage.get();
        if (rpc.getTerm() < this.term) {
            context.getConnector().replyInstallSnapshot(new InstallSnapshotResult(this.term), rpcMessage);
            return;
        }

        if (rpc.getTerm() > this.term) {
            this.cancelTimeoutOrTask();
            FollowerNodeRole newNodeState = new FollowerNodeRole(rpc.getTerm(), null, null, context.scheduleElectionTimeout());
            context.changeToNodeRole(newNodeState);
            newNodeState.processInstallSnapshotRpc(context, rpcMessage);
        } else {
            processInstallSnapshotRpc(context, rpcMessage);
        }
        context.getConnector().replyInstallSnapshot(new InstallSnapshotResult(rpc.getTerm()), rpcMessage);
    }

    protected void processInstallSnapshotRpc(NodeRoleContext context, InstallSnapshotRpcMessage rpcMessage) {
    }

    public void onReceiveInstallSnapshotResult(NodeRoleContext context, InstallSnapshotResult result, NodeId sourceNodeId, InstallSnapshotRpc rpc) {
        // TODO extract method
        if (result.getTerm() > this.term) {
            this.cancelTimeoutOrTask();
            context.changeToNodeRole(new FollowerNodeRole(result.getTerm(), null, null, context.scheduleElectionTimeout()));
            return;
        }

        processInstallSnapshotResult(context, result, sourceNodeId, rpc);
    }

    protected void processInstallSnapshotResult(NodeRoleContext context, InstallSnapshotResult result, NodeId sourceNodeId, InstallSnapshotRpc rpc) {
    }

}
