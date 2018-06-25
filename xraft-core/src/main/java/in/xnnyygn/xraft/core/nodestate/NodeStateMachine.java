package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.ReplicationStateTracker;
import in.xnnyygn.xraft.core.rpc.*;
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
    private AbstractNodeState nodeState;

    private final NodeGroup nodeGroup;
    private final NodeId selfNodeId;
    private final NodeStore nodeStore;
    private final Connector rpcConnector;

    private final Scheduler scheduler;
    private final List<NodeStateListener> nodeStateListeners = new ArrayList<>();

    public NodeStateMachine(NodeGroup nodeGroup, NodeId selfNodeId, NodeStore nodeStore, Connector rpcConnector) {
        this.nodeGroup = nodeGroup;
        this.selfNodeId = selfNodeId;
        this.nodeStore = nodeStore;
        this.rpcConnector = rpcConnector;

        this.scheduler = new Scheduler(selfNodeId);
    }

    public synchronized void start() {
        this.nodeState = new FollowerNodeState(this.nodeStore, this.scheduleElectionTimeout());
        logger.debug("Node {}, start with state {}", this.selfNodeId, this.nodeState);
    }

    public synchronized void onReceiveRequestVoteResult(RequestVoteResult result, NodeId senderNodeId) {
        logger.debug("Node {}, receive {} from peer {}", this.selfNodeId, result, senderNodeId);
        this.nodeState.onReceiveRequestVoteResult(this, result);
    }

    public synchronized void onReceiveRequestVoteRpc(RequestVoteRpc rpc) {
        logger.debug("Node {}, receive {} from peer {}", this.selfNodeId, rpc, rpc.getCandidateId());
        this.nodeState.onReceiveRequestVoteRpc(this, rpc);
    }

    public synchronized void onReceiveAppendEntriesRpc(AppendEntriesRpc rpc) {
        logger.debug("Node {}, receive {} from leader {}", this.selfNodeId, rpc, rpc.getLeaderId());
        this.nodeState.onReceiveAppendEntriesRpc(this, rpc);
    }

    private synchronized void onElectionTimeout() {
        logger.debug("Node {}, election timeout", this.selfNodeId);
        this.nodeState.electionTimeout(this);
    }

    private synchronized void replicateLog() {
        logger.debug("Node {}, replicate log", this.selfNodeId);
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(this.nodeState.getTerm());
        rpc.setLeaderId(this.selfNodeId);
        this.rpcConnector.sendRpc(rpc);
    }

    /**
     * Take snapshot of current node state(without null check).
     * DON'T call before startup or you will get NPE.
     *
     * @return node state snapshot
     */
    public NodeStateSnapshot takeSnapshot() {
        return this.nodeState.takeSnapshot();
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
    public ReplicationStateTracker createReplicationStateTracker() {
//        Set<NodeId> nodeIds = this.nodeGroup.getNodeIds();
//        nodeIds.remove(this.selfNodeId);
//        return new ReplicationStateTracker(nodeIds, this.nodeContext);
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeToNodeState(AbstractNodeState newNodeState) {
        logger.debug("Node {}, state changed {} -> {}", this.selfNodeId, this.nodeState, newNodeState);

        // notify listener if not stable
        if (!isStableBetween(this.nodeState, newNodeState)) {
            NodeStateSnapshot snapshot = newNodeState.takeSnapshot();
            this.nodeStateListeners.forEach((l) -> {
                l.nodeStateChanged(snapshot);
            });
        }

        this.nodeState = newNodeState;
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
    public ElectionTimeout scheduleElectionTimeout() {
        return this.scheduler.scheduleElectionTimeout(this::onElectionTimeout);
    }

    public void stop() throws InterruptedException {
        this.scheduler.stop();
    }

    public void addNodeStateListener(NodeStateListener listener) {
        this.nodeStateListeners.add(listener);
    }

    @Override
    public Log getLog() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connector getConnector() {
        return null;
    }

}
