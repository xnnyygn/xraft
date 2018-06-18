package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.RequestVoteRpc;
import in.xnnyygn.xraft.core.rpc.Router;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import in.xnnyygn.xraft.core.schedule.Scheduler;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.node.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class NodeStateMachine implements NodeStateContext {

    private static final Logger logger = LoggerFactory.getLogger(NodeStateMachine.class);
    private AbstractNodeState serverState;

    private final NodeGroup nodeGroup;
    private final NodeId selfNodeId;
    private final NodeStore nodeStore;
    private final Router rpcRouter;

    private final Scheduler scheduler;
    private final List<NodeStateListener> nodeStateListeners = new ArrayList<>();

    public NodeStateMachine(NodeGroup nodeGroup, NodeId selfNodeId, NodeStore nodeStore, Router rpcRouter) {
        this.nodeGroup = nodeGroup;
        this.selfNodeId = selfNodeId;
        this.nodeStore = nodeStore;
        this.rpcRouter = rpcRouter;

        this.scheduler = new Scheduler(selfNodeId);
    }

    public synchronized void start() {
        this.serverState = new FollowerNodeState(this.nodeStore, this.scheduleElectionTimeout());
        logger.debug("Node {}, start with state {}", this.selfNodeId, this.serverState);
    }

    public synchronized void onReceiveRequestVoteResult(RequestVoteResult result, NodeId senderNodeId) {
        logger.debug("Node {}, receive {} from peer {}", this.selfNodeId, result, senderNodeId);
        this.serverState.onReceiveRequestVoteResult(this, result);
    }

    public synchronized void onReceiveRequestVoteRpc(RequestVoteRpc rpc) {
        logger.debug("Node {}, receive {} from peer {}", this.selfNodeId, rpc, rpc.getCandidateId());
        this.serverState.onReceiveRequestVoteRpc(this, rpc);
    }

    public synchronized void onReceiveAppendEntriesRpc(AppendEntriesRpc rpc) {
        logger.debug("Node {}, receive {} from leader {}", this.selfNodeId, rpc, rpc.getLeaderId());
        this.serverState.onReceiveAppendEntriesRpc(this, rpc);
    }

    private synchronized void onElectionTimeout() {
        logger.debug("Node {}, election timeout", this.selfNodeId);
        this.serverState.onElectionTimeout(this);
    }

    private synchronized void replicateLog() {
        logger.debug("Node {}, replicate log", this.selfNodeId);
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(this.serverState.getTerm());
        rpc.setLeaderId(this.selfNodeId);
        this.rpcRouter.sendRpc(rpc);
    }

    /**
     * Take snapshot of current node state(without null check).
     * DON'T call before startup or you will get NPE.
     *
     * @return node state snapshot
     */
    public NodeStateSnapshot takeSnapshot() {
        return this.serverState.takeSnapshot();
    }

    @Override
    public NodeId getSelfNodeId() {
        return this.selfNodeId;
    }

    @Override
    public int getNodeCount() {
        return this.nodeGroup.getCount();
    }

    @Override
    public void setNodeState(AbstractNodeState nodeState) {
        logger.debug("Node {}, state changed {} -> {}", this.selfNodeId, this.serverState, nodeState);

        // notify listener if not stable
        if (!isStableBetween(this.serverState, nodeState)) {
            NodeStateSnapshot snapshot = nodeState.takeSnapshot();
            this.nodeStateListeners.forEach((l) -> {
                l.nodeStateChanged(snapshot);
            });
        }

        this.serverState = nodeState;
    }

    private boolean isStableBetween(AbstractNodeState before, AbstractNodeState after) {
        return before != null &&
                before.getRole() == NodeRole.FOLLOWER && after.getRole() == NodeRole.FOLLOWER &&
                FollowerNodeState.isStableBetween((FollowerNodeState) before, (FollowerNodeState) after);
    }

    @Override
    public LogReplicationTask scheduleLogReplicationTask() {
        return this.scheduler.scheduleLogReplicationTask(this::replicateLog);
    }

    @Override
    public Router getRpcRouter() {
        return this.rpcRouter;
    }

    @Override
    public ElectionTimeout scheduleElectionTimeout() {
        return this.scheduler.scheduleElectionTimeout(this::onElectionTimeout);
    }

    public void stop() throws InterruptedException {
        this.scheduler.stop();
    }

    public void addNodeStateListener(NodeStateListener listener) {
        this.nodeStateListeners.add(listener);
    }

}
