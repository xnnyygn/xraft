package in.xnnyygn.xraft.core.nodestate;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import in.xnnyygn.xraft.core.log.EntryApplier;
import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.ReplicationStateTracker;
import in.xnnyygn.xraft.core.node.NodeContext;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.*;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResultMessage;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpcMessage;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpcMessage;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NodeStateMachine implements NodeStateContext {

    private static final Logger logger = LoggerFactory.getLogger(NodeStateMachine.class);
    private final NodeContext nodeContext;
    private AbstractNodeState nodeState;
    private final List<NodeStateListener> nodeStateListeners = new ArrayList<>();
    private final ListeningExecutorService executorService;

    public NodeStateMachine(NodeContext nodeContext) {
        this.nodeContext = nodeContext;
        this.nodeContext.register(this);
        this.executorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(
                r -> new Thread(r, "node-state-machine-" + nodeContext.getSelfNodeId()))
        );
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

        this.runWithMonitor(() -> {
            this.nodeContext.getLog().appendEntry(this.nodeState.getTerm(), command, applier);
            this.nodeState.replicateLog(this);
        });
    }

    public void addNodeStateListener(NodeStateListener listener) {
        this.nodeStateListeners.add(listener);
    }

    public void stop() {
        this.nodeState.cancelTimeoutOrTask();

        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(1L, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    @Subscribe
    public void onReceive(RequestVoteRpcMessage rpcMessage) {
        this.runWithMonitor(() -> {
            logger.debug("receive {} from {}", rpcMessage.get(), rpcMessage.getSourceNodeId());
            this.nodeState.onReceiveRequestVoteRpc(this, rpcMessage);
        });
    }

    @Subscribe
    public void onReceive(RequestVoteResult result) {
        this.runWithMonitor(() -> {
            logger.debug("receive {}", result);
            this.nodeState.onReceiveRequestVoteResult(this, result);
        });
    }

    @Subscribe
    public void onReceive(AppendEntriesRpcMessage rpcMessage) {
        this.runWithMonitor(() -> {
            logger.debug("receive {} from {}", rpcMessage.get(), rpcMessage.getSourceNodeId());
            this.nodeState.onReceiveAppendEntriesRpc(this, rpcMessage);
        });
    }

    @Subscribe
    public void onReceive(AppendEntriesResultMessage message) {
        this.runWithMonitor(() -> {
            logger.debug("receive {} from {}", message.getResult(), message.getSourceNodeId());
            this.nodeState.onReceiveAppendEntriesResult(this, message.getResult(), message.getSourceNodeId(), message.getRpc());
        });
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
        return this.nodeContext.getScheduler().scheduleLogReplicationTask(
                () -> this.runWithMonitor(() -> this.nodeState.replicateLog(this))
        );
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
        return this.nodeContext.getScheduler().scheduleElectionTimeout(
                () -> this.runWithMonitor(() -> this.nodeState.electionTimeout(this))
        );
    }

    private void runWithMonitor(Runnable r) {
        this.nodeContext.runWithMonitor(this.executorService, r);
    }

}
