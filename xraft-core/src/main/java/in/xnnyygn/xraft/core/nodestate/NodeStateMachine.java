package in.xnnyygn.xraft.core.nodestate;

import com.google.common.eventbus.Subscribe;
import in.xnnyygn.xraft.core.log.EntryApplier;
import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.ReplicationStateTracker;
import in.xnnyygn.xraft.core.node.NodeContext;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.*;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NodeStateMachine implements NodeStateContext {

    private static final Logger logger = LoggerFactory.getLogger(NodeStateMachine.class);
    private final NodeContext nodeContext;
    private AbstractNodeState nodeState;
    private final List<NodeStateListener> nodeStateListeners = new ArrayList<>();

    public NodeStateMachine(NodeContext nodeContext) {
        this.nodeContext = nodeContext;
        this.nodeContext.register(this);
    }

    public void start() {
        this.changeToNodeState(new FollowerNodeState(this.nodeContext.getNodeStore(), this.scheduleElectionTimeout()));
    }

    public NodeStateSnapshot getNodeState() {
        return this.nodeState.takeSnapshot();
    }

    public void appendLog(byte[] command, EntryApplier applier) {
        if (this.nodeState.getRole() != NodeRole.LEADER) {
            throw new IllegalStateException("only leader can append log");
        }

        this.nodeContext.getLog().appendEntry(this.nodeState.getTerm(), command, applier);
        this.nodeState.replicateLog(this);
    }

    public void addNodeStateListener(NodeStateListener listener) {
        this.nodeStateListeners.add(listener);
    }

    public void stop() {
        this.nodeState.cancelTimeoutOrTask();
    }

    @Subscribe
    public void onReceive(RequestVoteRpc rpc) {
        logger.debug("receive {}", rpc);
        this.nodeState.onReceiveRequestVoteRpc(this, rpc);
    }

    @Subscribe
    public void onReceive(RequestVoteResult result) {
        logger.debug("receive {}", result);
        this.nodeState.onReceiveRequestVoteResult(this, result);
    }

    @Subscribe
    public void onReceive(AppendEntriesRpc rpc) {
        logger.debug("receive {}", rpc);
        this.nodeState.onReceiveAppendEntriesRpc(this, rpc);
    }

    @Subscribe
    public void onReceive(AppendEntriesResultMessage message) {
        logger.debug("receive {}", message.getResult());
        this.nodeState.onReceiveAppendEntriesResult(this, message.getResult(), message.getSourceNodeId(), message.getRpc());
    }

    @Override
    public NodeId getSelfNodeId() {
        return this.nodeContext.getSelfNodeId();
    }

    @Override
    public int getNodeCount() {
        return this.nodeContext.getNodeGroup().getCount();
    }

    @Override
    public ReplicationStateTracker createReplicationStateTracker() {
        Set<NodeId> nodeIds = new HashSet<>(this.nodeContext.getNodeGroup().getNodeIds());
        nodeIds.remove(this.nodeContext.getSelfNodeId());
        return new ReplicationStateTracker(nodeIds, this.nodeContext.getLog().getLastLogIndex() + 1);
    }

    @Override
    public void changeToNodeState(AbstractNodeState newNodeState) {
        logger.debug("Node {}, state changed {} -> {}", this.getSelfNodeId(), this.nodeState, newNodeState);

        // notify listener if not stable
        if (!isStableBetween(this.nodeState, newNodeState)) {
            NodeStateSnapshot snapshot = newNodeState.takeSnapshot();
            this.nodeStateListeners.forEach((l) -> l.nodeStateChanged(snapshot));
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
        return this.nodeContext.getScheduler().scheduleLogReplicationTask(() -> this.nodeState.replicateLog(this));
    }

    @Override
    public Log getLog() {
        return this.nodeContext.getLog();
    }

    @Override
    public Connector getConnector() {
        return this.nodeContext.getConnector();
    }

    @Override
    public ElectionTimeout scheduleElectionTimeout() {
        return this.nodeContext.getScheduler().scheduleElectionTimeout(() -> this.nodeState.electionTimeout(this));
    }

}
