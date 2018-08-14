package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import in.xnnyygn.xraft.core.log.*;
import in.xnnyygn.xraft.core.log.entry.*;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryBatchRemovedEvent;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryCommittedEvent;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryFromLeaderAppendEvent;
import in.xnnyygn.xraft.core.noderole.*;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.rpc.message.*;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class Controller implements NodeRoleContext {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    private final List<NodeRoleListener> nodeRoleListeners = new ArrayList<>();
    private final TaskReferenceHolder taskReferenceHolder = new TaskReferenceHolder();

    private final NodeContext nodeContext;
    private AbstractNodeRole nodeRole;

    private final ListeningExecutorService executorService;

    Controller(NodeContext nodeContext) {
        this.nodeContext = nodeContext;
        this.nodeContext.register(this);
        this.executorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(
                r -> new Thread(r, "controller-" + nodeContext.getSelfNodeId()))
        );
    }

    void start() {
        nodeContext.initialize();
        this.changeToNodeRole(new FollowerNodeRole(nodeContext.getNodeStore(), scheduleElectionTimeout()));
    }

    RoleStateSnapshot getRoleState() {
        return this.nodeRole.takeSnapshot();
    }

    void registerStateMachine(StateMachine stateMachine) {
        this.nodeContext.getLog().setStateMachine(stateMachine);
    }

    void appendLog(byte[] command) {
        ensureLeader();
        this.runWithMonitor(() -> {
            this.nodeContext.getLog().appendEntry(this.nodeRole.getTerm(), command);
            ((LeaderNodeRole) this.nodeRole).replicateLog(this);
        });
    }

    private void ensureLeader() {
        if (this.nodeRole.getRole() != RoleName.LEADER) {
            throw new IllegalStateException("only leader can perform such operation");
        }
    }

    TaskReference addServer(NodeEndpoint config) {
        ensureLeader();
        AddNodeTaskReference taskReference = new AddNodeTaskReference(config.getId());
        runWithMonitor(() -> {
            if (!taskReferenceHolder.awaitPreviousGroupConfigChange(nodeContext.getLog().getLastUncommittedGroupConfigEntry(), taskReference, 1000)) {
                return;
            }
            nodeContext.addNode(config, false);
            ((LeaderNodeRole) nodeRole).replicateLog(this, config.getId());
            taskReferenceHolder.set(taskReference);
        });
        return taskReference;
    }

    TaskReference removeServer(NodeId id) {
        ensureLeader();
        EntryTaskReference taskReference = new EntryTaskReference();
        runWithMonitor(() -> {
            if (!taskReferenceHolder.awaitPreviousGroupConfigChange(nodeContext.getLog().getLastUncommittedGroupConfigEntry(), taskReference, 1000)) {
                return;
            }
            Set<NodeEndpoint> nodeEndpoints = nodeContext.getNodeGroup().getNodeEndpointsOfMajor();
            RemoveNodeEntry entry = nodeContext.getLog().appendEntryForRemoveNode(nodeRole.getTerm(), nodeEndpoints, id);
            taskReference.setEntryIndex(entry.getIndex());
            taskReferenceHolder.set(taskReference);
            nodeContext.getNodeGroup().downgrade(id);
            ((LeaderNodeRole) nodeRole).replicateLog(this);
        });
        return taskReference;
    }


    void addNodeRoleListener(NodeRoleListener listener) {
        this.nodeRoleListeners.add(listener);
    }

    void stop() throws InterruptedException {
        this.nodeRole.cancelTimeoutOrTask();

        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(1L, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }

        this.nodeContext.release();
    }

    @Subscribe
    public void onReceive(RequestVoteRpcMessage rpcMessage) {
        this.runWithMonitor(() -> {
            this.nodeRole.onReceiveRequestVoteRpc(this, rpcMessage);
        });
    }

    @Subscribe
    public void onReceive(RequestVoteResult result) {
        this.runWithMonitor(() -> {
            this.nodeRole.onReceiveRequestVoteResult(this, result);
        });
    }

    @Subscribe
    public void onReceive(AppendEntriesRpcMessage rpcMessage) {
        this.runWithMonitor(() -> {
            this.nodeRole.onReceiveAppendEntriesRpc(this, rpcMessage);
        });
    }

    @Subscribe
    public void onReceive(AppendEntriesResultMessage resultMessage) {
        this.runWithMonitor(() -> {
            this.nodeRole.onReceiveAppendEntriesResult(this, resultMessage.get(), resultMessage.getSourceNodeId(), resultMessage.getRpc());
        });
    }

    @Subscribe
    public void onReceive(InstallSnapshotRpcMessage rpcMessage) {
        this.runWithMonitor(() -> {
            this.nodeRole.onReceiveInstallSnapshotRpc(this, rpcMessage);
        });
    }

    @Subscribe
    public void onReceive(InstallSnapshotResultMessage resultMessage) {
        runWithMonitor(() -> {
            // TODO add channel to result message
            this.nodeRole.onReceiveInstallSnapshotResult(this, resultMessage.get(), resultMessage.getSourceNodeId(), resultMessage.getRpc());
        });
    }

    @Subscribe
    public void onReceive(GroupConfigEntryFromLeaderAppendEvent event) {
        runWithMonitor(() -> {
            GroupConfigEntry entry = event.getEntry();
            nodeContext.getNodeGroup().updateNodes(entry.getResultNodeConfigs());
        });
    }

    @Subscribe
    public void onReceive(GroupConfigEntryCommittedEvent event) {
        runWithMonitor(() -> {
            GroupConfigEntry entry = event.getEntry();
            if (entry.getKind() == Entry.KIND_REMOVE_NODE) {
                RemoveNodeEntry removeNodeEntry = (RemoveNodeEntry) entry;
                NodeId nodeToRemove = removeNodeEntry.getNodeToRemove();
                if (nodeToRemove.equals(nodeContext.getSelfNodeId())) {
                    logger.info("remove leader from group, step down");
                    nodeRole.stepDown(Controller.this, true);
                }
                nodeContext.getNodeGroup().removeNode(nodeToRemove);
            }
            taskReferenceHolder.done(entry.getIndex());
        });
    }

    @Subscribe
    public void onReceive(GroupConfigEntryBatchRemovedEvent event) {
        runWithMonitor(() -> {
            GroupConfigEntry entry = event.getFirstRemovedEntry();
            nodeContext.getNodeGroup().updateNodes(entry.getNodeEndpoints());
        });
    }

    @Override
    public NodeId getSelfNodeId() {
        return this.nodeContext.getSelfNodeId();
    }

    @Override
    public NodeGroup getNodeGroup() {
        return nodeContext.getNodeGroup();
    }

    @Override
    public void resetReplicationStates() {
        nodeContext.resetReplicationStates();
    }

    // called when catch up successfully
    @Override
    public void upgradeNode(NodeId id) {
        NodeEndpoint newNodeEndpoint = nodeContext.getNodeGroup().findEndpoint(id);
        AddNodeEntry entry = nodeContext.getLog().appendEntryForAddNode(nodeRole.getTerm(), nodeContext.getNodeGroup().getNodeEndpointsOfMajor(), newNodeEndpoint);
        nodeContext.getNodeGroup().upgrade(id);
        taskReferenceHolder.setEntryIndexForAddNode(entry.getIndex());
    }

    // called when failed to catch up
    @Override
    public void removeNode(NodeId id) {
        nodeContext.getNodeGroup().removeNode(id);
        taskReferenceHolder.doneForAddNode(TaskReference.Result.TIMEOUT);
    }

    @Override
    public void changeToNodeRole(AbstractNodeRole newNodeRole) {
        logger.debug("node {}, state changed {} -> {}", this.getSelfNodeId(), this.nodeRole, newNodeRole);

        // notify listener if not stable
        if (!isStableBetween(this.nodeRole, newNodeRole)) {
            RoleStateSnapshot snapshot = newNodeRole.takeSnapshot();
            this.nodeRoleListeners.forEach((l) -> l.nodeRoleChanged(snapshot));

            NodeStore store = nodeContext.getNodeStore();
            store.setTerm(snapshot.getTerm());
            store.setVotedFor(snapshot.getVotedFor());
        }

        this.nodeRole = newNodeRole;
    }

    private boolean isStableBetween(AbstractNodeRole before, AbstractNodeRole after) {
        return before != null &&
                before.getRole() == RoleName.FOLLOWER && after.getRole() == RoleName.FOLLOWER &&
                FollowerNodeRole.isStableBetween((FollowerNodeRole) before, (FollowerNodeRole) after);
    }

    @Override
    public LogReplicationTask scheduleLogReplicationTask() {
        return this.nodeContext.getScheduler().scheduleLogReplicationTask(
                () -> this.runWithMonitor(() -> ((LeaderNodeRole) this.nodeRole).replicateLog(this))
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
                () -> this.runWithMonitor(() -> this.nodeRole.electionTimeout(this))
        );
    }

    @Override
    public boolean isStandbyMode() {
        return nodeContext.isStandbyMode();
    }

    @SuppressWarnings("UnusedReturnValue")
    private Future runWithMonitor(Runnable r) {
        return nodeContext.runWithMonitor(executorService, r);
    }

}
