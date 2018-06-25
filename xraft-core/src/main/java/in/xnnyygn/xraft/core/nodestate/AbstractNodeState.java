package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract node state.
 */
public abstract class AbstractNodeState {

    public static final int ALL_ENTRIES = -1;

    private static final Logger logger = LoggerFactory.getLogger(AbstractNodeState.class);
    protected final NodeRole role;
    protected final int term;

    /**
     * Create.
     *
     * @param role role
     * @param term term
     */
    AbstractNodeState(NodeRole role, int term) {
        this.role = role;
        this.term = term;
    }

    /**
     * Get role.
     *
     * @return role
     */
    public NodeRole getRole() {
        return role;
    }

    public abstract NodeStateSnapshot takeSnapshot();

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
    protected abstract void cancelTimeoutOrTask();

    /**
     * Called when election timeout.
     *
     * @param context context
     */
    public void electionTimeout(NodeStateContext context) {
        if (this.role == NodeRole.LEADER) {
            logger.warn("Node {}, current role is LEADER, ignore election timeout", context.getSelfNodeId());
            return;
        }

        // follower: start election
        // candidate: restart election
        int newTerm = this.term + 1;

        // reset election timeout
        this.cancelTimeoutOrTask();
        context.changeToNodeState(new CandidateNodeState(newTerm, context.scheduleElectionTimeout()));

        // rpc
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(newTerm);
        rpc.setCandidateId(context.getSelfNodeId());
        context.getConnector().sendRpc(rpc);
    }

    public void replicateLog(NodeStateContext context) {
        this.replicateLog(context, ALL_ENTRIES);
    }

    public void replicateLog(NodeStateContext context, int maxEntries) {
        // do nothing
    }

    /**
     * Called when receive request vote rpc.
     *
     * @param context context
     * @param rpc     rpc
     */
    public void onReceiveRequestVoteRpc(NodeStateContext context, RequestVoteRpc rpc) {
        RequestVoteResult result;

        if (rpc.getTerm() > this.term) {
            boolean voteForCandidate = !context.getLog().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm());
            this.cancelTimeoutOrTask();
            context.changeToNodeState(new FollowerNodeState(rpc.getTerm(), (voteForCandidate ? rpc.getCandidateId() : null), null, context.scheduleElectionTimeout()));
            result = new RequestVoteResult(rpc.getTerm(), voteForCandidate);
        } else if (rpc.getTerm() == this.term) {
            result = processRequestVoteRpc(context, rpc);
        } else {
            result = new RequestVoteResult(this.term, false);
        }

        context.getConnector().sendResult(result, rpc.getCandidateId());
    }

    /**
     * Process request vote rpc in same term.
     *
     * @param context context
     * @param rpc     rpc
     * @return request vote result
     */
    protected abstract RequestVoteResult processRequestVoteRpc(NodeStateContext context, RequestVoteRpc rpc);

    /**
     * Called when receive request vote result.
     *
     * @param context context
     * @param result  result
     */
    public void onReceiveRequestVoteResult(NodeStateContext context, RequestVoteResult result) {
        if (result.getTerm() > this.term) {
            this.cancelTimeoutOrTask();
            context.changeToNodeState(new FollowerNodeState(result.getTerm(), null, null, context.scheduleElectionTimeout()));
            return;
        }

        processRequestVoteResult(context, result);
    }

    protected void processRequestVoteResult(NodeStateContext context, RequestVoteResult result) {
    }

    /**
     * Called when receive append entries rpc.
     *
     * @param context context
     * @param rpc     rpc
     */
    public void onReceiveAppendEntriesRpc(NodeStateContext context, AppendEntriesRpc rpc) {
        AppendEntriesResult result;

        if (rpc.getTerm() > this.term) {
            this.cancelTimeoutOrTask();
            context.changeToNodeState(new FollowerNodeState(rpc.getTerm(), null, rpc.getLeaderId(), context.scheduleElectionTimeout()));
            result = new AppendEntriesResult(rpc.getTerm(), true);
        } else if (rpc.getTerm() == this.term) {
            result = processAppendEntriesRpc(context, rpc);
        } else {
            result = new AppendEntriesResult(this.term, false);
        }

        context.getConnector().sendAppendEntriesResult(result, rpc.getLeaderId(), rpc);
    }

    /**
     * Process append entries rpc in same term.
     *
     * @param context context
     * @param rpc     rpc
     * @return append entries result
     */
    protected abstract AppendEntriesResult processAppendEntriesRpc(NodeStateContext context, AppendEntriesRpc rpc);

    public void onReceiveAppendEntriesResult(NodeStateContext context, AppendEntriesResult result, NodeId sourceNodeId, AppendEntriesRpc rpc) {
        if (result.getTerm() > this.term) {
            this.cancelTimeoutOrTask();
            context.changeToNodeState(new FollowerNodeState(result.getTerm(), null, null, context.scheduleElectionTimeout()));
            return;
        }

        processAppendEntriesResult(context, result, sourceNodeId, rpc);
    }

    protected void processAppendEntriesResult(NodeStateContext context, AppendEntriesResult result, NodeId sourceNodeId, AppendEntriesRpc rpc) {
    }


}
