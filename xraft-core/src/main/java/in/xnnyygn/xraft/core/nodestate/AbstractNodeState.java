package in.xnnyygn.xraft.core.nodestate;

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
    public void onElectionTimeout(NodeStateContext context) {
        if (this.role == NodeRole.LEADER) {
            logger.warn("Node {}, current role is LEADER, ignore", context.getSelfNodeId());
            return;
        }

        // follower: start election
        // candidate: restart election
        int newTerm = this.term + 1;

        // reset election timeout
        this.cancelTimeoutOrTask();
        context.setNodeState(new CandidateNodeState(newTerm, context.scheduleElectionTimeout()));

        // rpc
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(newTerm);
        rpc.setCandidateId(context.getSelfNodeId());
        context.getRpcConnector().sendRpc(rpc);
    }

    /**
     * Called when receive request vote result.
     *
     * @param context context
     * @param result  result
     */
    public abstract void onReceiveRequestVoteResult(NodeStateContext context, RequestVoteResult result);

    /**
     * Called when receive request vote rpc.
     *
     * @param context context
     * @param rpc     rpc
     */
    public void onReceiveRequestVoteRpc(NodeStateContext context, RequestVoteRpc rpc) {
        RequestVoteResult result;

        if (rpc.getTerm() < this.term) {

            // peer's term is old
            result = new RequestVoteResult(this.term, false);
        } else if (rpc.getTerm() == this.term) {
            result = processRequestVoteRpc(context, rpc);
        } else {

            // peer's term > current term
            logger.debug("Node {}, update to peer {}'s term {} and vote for it", context.getSelfNodeId(), rpc.getCandidateId(), rpc.getTerm());
            this.cancelTimeoutOrTask();
            context.setNodeState(new FollowerNodeState(rpc.getTerm(), rpc.getCandidateId(), null, context.scheduleElectionTimeout()));
            result = new RequestVoteResult(rpc.getTerm(), true);
        }

        context.getRpcConnector().sendResult(result, rpc.getCandidateId());
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
     * Called when receive append entries rpc.
     *
     * @param context context
     * @param rpc     rpc
     */
    public void onReceiveAppendEntriesRpc(NodeStateContext context, AppendEntriesRpc rpc) {
        AppendEntriesResult result;

        if (rpc.getTerm() < this.term) {

            // peer's term is old
            result = new AppendEntriesResult(this.term, false);
        } else if (rpc.getTerm() == this.term) {
            result = processAppendEntriesRpc(context, rpc);
        } else {

            // leader's term > current term
            this.cancelTimeoutOrTask();
            context.setNodeState(new FollowerNodeState(rpc.getTerm(), null, rpc.getLeaderId(), context.scheduleElectionTimeout()));
            result = new AppendEntriesResult(rpc.getTerm(), true);
        }

        context.getRpcConnector().sendResult(result, rpc.getLeaderId());
    }

    /**
     * Process append entries rpc in same term.
     *
     * @param context context
     * @param rpc     rpc
     * @return append entries result
     */
    protected abstract AppendEntriesResult processAppendEntriesRpc(NodeStateContext context, AppendEntriesRpc rpc);

}
