package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import in.xnnyygn.xraft.core.log.StateMachine;
import in.xnnyygn.xraft.core.log.entry.EntryMeta;
import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryBatchRemovedEvent;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryCommittedEvent;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryFromLeaderAppendEvent;
import in.xnnyygn.xraft.core.log.replication.ReplicatingState;
import in.xnnyygn.xraft.core.log.snapshot.EntryInSnapshotException;
import in.xnnyygn.xraft.core.node.task.*;
import in.xnnyygn.xraft.core.rpc.message.*;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class NodeImpl implements Node {

    private static final Logger logger = LoggerFactory.getLogger(NodeImpl.class);
    private static final FutureCallback<Object> LOGGING_FUTURE_CALLBACK = new FutureCallback<Object>() {
        @Override
        public void onSuccess(@Nullable Object result) {
        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
            logger.warn("failure", t);
        }
    };

    private final NodeContext context;
    private volatile AbstractNodeRole role;
    private final List<NodeRoleListener> roleListeners = new ArrayList<>();
    private final NewNodeCatchUpTaskContext newNodeCatchUpTaskContext = new NewNodeCatchUpTaskContextImpl();
    private final NewNodeCatchUpTaskGroup newNodeCatchUpTaskGroup = new NewNodeCatchUpTaskGroup();
    private final GroupConfigChangeTaskContext groupConfigChangeTaskContext = new GroupConfigChangeTaskContextImpl();
    private volatile GroupConfigChangeTaskHolder groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder();

    NodeImpl(NodeContext context) {
        this.context = context;
    }

    public NodeContext getContext() {
        return context;
    }

    @Override
    public void registerStateMachine(StateMachine stateMachine) {
        context.log().setStateMachine(stateMachine);
    }

    @Override
    public RoleNameAndLeaderId getRoleNameAndLeaderId() {
        return role.getNameAndLeaderId(context.selfId());
    }

    @Override
    public RoleState getRoleState() {
        return role.getState();
    }

    @Override
    public void addNodeRoleListener(NodeRoleListener listener) {
        roleListeners.add(listener);
    }

    @Override
    public void start() {
        context.eventBus().register(this);
        changeToRole(new FollowerNodeRole(context.store().getTerm(), context.store().getVotedFor(), null,
                scheduleElectionTimeout()));
        context.connector().initialize();
    }

    @Override
    public void appendLog(byte[] commandBytes) {
        ensureLeader();
        context.taskExecutor().submit(() -> {
            context.log().appendEntry(role.getTerm(), commandBytes);
            doReplicateLog();
        }, LOGGING_FUTURE_CALLBACK);
    }

    @Override
    public GroupConfigChangeTaskReference addNode(NodeEndpoint newNodeEndpoint) {
        ensureLeader();
        // TODO add test
        if (context.selfId().equals(newNodeEndpoint.getId())) {
            throw new IllegalArgumentException("new node id cannot be self id");
        }
        NewNodeCatchUpTask newNodeCatchUpTask = new NewNodeCatchUpTask(newNodeCatchUpTaskContext, newNodeEndpoint, context.config());
        // TODO test same node id
        if (!newNodeCatchUpTaskGroup.add(newNodeCatchUpTask)) {
            throw new IllegalArgumentException("node " + newNodeEndpoint.getId() + " is adding");
        }
        NewNodeCatchUpTaskResult newNodeCatchUpTaskResult;
        try {
            newNodeCatchUpTaskResult = newNodeCatchUpTask.call();
            switch (newNodeCatchUpTaskResult.getState()) {
                case REPLICATION_FAILED:
                    return new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.REPLICATION_FAILED);
                case TIMEOUT:
                    return new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.TIMEOUT);
            }
        } catch (Exception e) {
            logger.warn("failed to catch up new node " + newNodeEndpoint.getId(), e);
            return new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.ERROR);
        }
        GroupConfigChangeTaskResult result = awaitPreviousGroupConfigChangeTask();
        if(result != null) {
            return new FixedResultGroupConfigTaskReference(result);
        }
        synchronized (this) {
            if (!groupConfigChangeTaskHolder.isEmpty()) {
                throw new IllegalStateException("group config change concurrently");
            }
            AddNodeTask task = new AddNodeTask(groupConfigChangeTaskContext, newNodeEndpoint, newNodeCatchUpTaskResult.getNextIndex(), newNodeCatchUpTaskResult.getMatchIndex());
            Future<GroupConfigChangeTaskResult> future = context.groupConfigChangeTaskExecutor().submit(task);
            GroupConfigChangeTaskReference reference = new FutureGroupConfigChangeTaskReference(future);
            groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder(task, reference);
            return reference;
        }
    }

    private GroupConfigChangeTaskResult awaitPreviousGroupConfigChangeTask() {
        try {
            groupConfigChangeTaskHolder.awaitDone(context.config().getPreviousGroupConfigChangeTimeout());
            return null;
        } catch (InterruptedException ignored) {
            return GroupConfigChangeTaskResult.ERROR;
        }catch (TimeoutException ignored) {
            logger.info("previous cannot complete within timeout");
            return GroupConfigChangeTaskResult.TIMEOUT;
        }
    }

    private void ensureLeader() {
        RoleNameAndLeaderId roleNameAndLeaderId = role.getNameAndLeaderId(context.selfId());
        if (roleNameAndLeaderId.getRoleName() != RoleName.LEADER) {
            throw new NotLeaderException(roleNameAndLeaderId.getRoleName(), roleNameAndLeaderId.getLeaderId());
        }
    }

    @Override
    public GroupConfigChangeTaskReference removeNode(NodeId id) {
        ensureLeader();
        GroupConfigChangeTaskResult result = awaitPreviousGroupConfigChangeTask();
        if(result != null) {
            return new FixedResultGroupConfigTaskReference(result);
        }
        synchronized (this) {
            if (!groupConfigChangeTaskHolder.isEmpty()) {
                throw new IllegalStateException("group config change concurrently");
            }
            RemoveNodeTask task = new RemoveNodeTask(groupConfigChangeTaskContext, id);
            Future<GroupConfigChangeTaskResult> future = context.groupConfigChangeTaskExecutor().submit(task);
            GroupConfigChangeTaskReference reference = new FutureGroupConfigChangeTaskReference(future);
            groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder(task, reference);
            return reference;
        }
    }

    synchronized void cancelGroupConfigChangeTask() {
        if (groupConfigChangeTaskHolder.isEmpty()) {
            return;
        }
        logger.info("cancel group config change task");
        groupConfigChangeTaskHolder.cancel();
        groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder();
    }

    /**
     * event: election timeout
     */
    void electionTimeout() {
        context.taskExecutor().submit(this::doProcessElectionTimeout, LOGGING_FUTURE_CALLBACK);
    }

    private void doProcessElectionTimeout() {
        if (role.getName() == RoleName.LEADER) {
            logger.warn("node {}, current role is leader, ignore election timeout", context.selfId());
            return;
        }

        // follower: start election
        // candidate: restart election
        int newTerm = role.getTerm() + 1;
        role.cancelTimeoutOrTask();

        if (context.group().isUniqueNode(context.selfId())) {
            if (context.mode() == NodeMode.STANDBY) {
                logger.info("starts with standby mode, skip election");
            } else {
                logger.info("no other node, just become leader");
                resetReplicationStates();
                changeToRole(new LeaderNodeRole(newTerm, scheduleLogReplicationTask()));
                context.log().appendEntry(newTerm); // no-op log
            }
        } else {
            changeToRole(new CandidateNodeRole(newTerm, scheduleElectionTimeout()));
            EntryMeta lastEntryMeta = context.log().getLastEntryMeta();
            RequestVoteRpc rpc = new RequestVoteRpc();
            rpc.setTerm(newTerm);
            rpc.setCandidateId(context.selfId());
            rpc.setLastLogIndex(lastEntryMeta.getIndex());
            rpc.setLastLogTerm(lastEntryMeta.getTerm());
            context.connector().sendRequestVote(rpc);
        }
    }

    private void becomeFollower(int term, NodeId votedFor, NodeId leaderId, boolean scheduleElectionTimeout) {
        role.cancelTimeoutOrTask();
        ElectionTimeout electionTimeout = scheduleElectionTimeout ? scheduleElectionTimeout() : ElectionTimeout.NONE;
        changeToRole(new FollowerNodeRole(term, votedFor, leaderId, electionTimeout));
    }

    private void changeToRole(AbstractNodeRole newRole) {
        if (!isStableBetween(role, newRole)) {
            logger.debug("node {}, state changed -> {}", context.selfId(), newRole);
            RoleState state = newRole.getState();
            NodeStore store = context.store();
            store.setTerm(state.getTerm());
            store.setVotedFor(state.getVotedFor());
            roleListeners.forEach(l -> l.nodeRoleChanged(state));
        }
        role = newRole;
    }

    private boolean isStableBetween(AbstractNodeRole before, AbstractNodeRole after) {
        return before != null &&
                before.getName() == RoleName.FOLLOWER && after.getName() == RoleName.FOLLOWER &&
                FollowerNodeRole.isStableBetween((FollowerNodeRole) before, (FollowerNodeRole) after);
    }

    private ElectionTimeout scheduleElectionTimeout() {
        return context.scheduler().scheduleElectionTimeout(this::electionTimeout);
    }

    private void resetReplicationStates() {
        context.group().resetReplicationStates(context.selfId(), context.log());
    }

    private LogReplicationTask scheduleLogReplicationTask() {
        return context.scheduler().scheduleLogReplicationTask(this::replicateLog);
    }

    /**
     * event: log replication.
     */
    void replicateLog() {
        context.taskExecutor().submit(this::doReplicateLog, LOGGING_FUTURE_CALLBACK);
    }

    private void doReplicateLog() {
        if (context.group().isUniqueNode(context.selfId())) {
            context.log().advanceCommitIndex(context.log().getNextIndex() - 1, role.getTerm());
            return;
        }
        logger.debug("replicate log");
        for (ReplicatingState replicatingState : context.group().getReplicationTargets()) {
            if (replicatingState.isReplicating() &&
                    System.currentTimeMillis() - replicatingState.getLastReplicatedAt() <= context.config().getMinReplicationInterval()) {
                logger.debug("node {} is replicating, skip replication task", replicatingState.getNodeId());
            } else {
                doReplicateLog(replicatingState, context.config().getMaxReplicationEntries());
            }
        }
    }

    private void doReplicateLog(NodeId id, int maxEntries) {
        ReplicatingState replicatingState = context.group().findReplicationState(id);
        doReplicateLog(replicatingState, maxEntries);
    }

    private void doReplicateLog(ReplicatingState replicatingState, int maxEntries) {
        replicatingState.startReplicating();
        try {
            AppendEntriesRpc rpc = context.log().createAppendEntriesRpc(role.getTerm(), context.selfId(), replicatingState.getNextIndex(), maxEntries);
            context.connector().sendAppendEntries(rpc, replicatingState.getNodeId());
        } catch (EntryInSnapshotException e) {
            logger.debug("log entry {} in snapshot, replicate with install snapshot RPC", replicatingState.getNextIndex());
            InstallSnapshotRpc rpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(), 0, context.config().getSnapshotDataLength());
            context.connector().sendInstallSnapshot(rpc, replicatingState.getNodeId());
        }
    }

    @Subscribe
    public void onReceiveRequestVoteRpc(RequestVoteRpcMessage rpcMessage) {
        context.taskExecutor().submit(() ->
                        context.connector().replyRequestVote(doProcessRequestVoteRpc(rpcMessage), rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private RequestVoteResult doProcessRequestVoteRpc(RequestVoteRpcMessage rpcMessage) {
        if (!context.group().isMemberOfMajor(rpcMessage.getSourceNodeId())) {
            logger.warn("receive request vote rpc from node {} which is not major node, ignore", rpcMessage.getSourceNodeId());
            return new RequestVoteResult(role.getTerm(), false);
        }
        RequestVoteRpc rpc = rpcMessage.get();
        if (rpc.getTerm() < role.getTerm()) {
            logger.debug("term from rpc < current term, don't vote ({} < {})", rpc.getTerm(), role.getTerm());
            return new RequestVoteResult(role.getTerm(), false);
        }
        // rpc from new candidate
        if (rpc.getTerm() > role.getTerm()) {
            boolean voteForCandidate = !context.log().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm());
            becomeFollower(rpc.getTerm(), (voteForCandidate ? rpc.getCandidateId() : null), null, true);
            return new RequestVoteResult(rpc.getTerm(), voteForCandidate);
        }
        assert rpc.getTerm() == role.getTerm();
        switch (role.getName()) {
            case FOLLOWER:
                FollowerNodeRole follower = (FollowerNodeRole) role;
                NodeId votedFor = follower.getVotedFor();
                if ((votedFor == null && !context.log().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm())) ||
                        Objects.equals(votedFor, rpc.getCandidateId())) {
                    // vote for candidate
                    becomeFollower(role.getTerm(), rpc.getCandidateId(), null, true);
                    return new RequestVoteResult(rpc.getTerm(), true);
                }
                return new RequestVoteResult(role.getTerm(), false);
            case CANDIDATE: // voted for self
            case LEADER:
                return new RequestVoteResult(role.getTerm(), false);
            default:
                throw new IllegalStateException("unexpected node role [" + role.getName() + "]");
        }
    }

    // TODO change to RequestVoteResultMessage
    @Subscribe
    public void onReceiveRequestVoteResult(RequestVoteResult result) {
        context.taskExecutor().submit(() -> doProcessRequestVoteResult(result), LOGGING_FUTURE_CALLBACK);
    }

    Future<?> processRequestVoteResult(RequestVoteResult result) {
        return context.taskExecutor().submit(() -> doProcessRequestVoteResult(result));
    }

    private void doProcessRequestVoteResult(RequestVoteResult result) {
        if (result.getTerm() > role.getTerm()) {
            becomeFollower(result.getTerm(), null, null, true);
            return;
        }
        if (role.getName() != RoleName.CANDIDATE) {
            logger.debug("receive request vote result and current role is not candidate, ignore");
            return;
        }
        if (!result.isVoteGranted()) {
            return;
        }
        int currentVotesCount = ((CandidateNodeRole) role).getVotesCount() + 1;
        int countOfMajor = context.group().getCountOfMajor();
        logger.debug("votes count {}, major node count {}", currentVotesCount, countOfMajor);
        role.cancelTimeoutOrTask();
        if (currentVotesCount > countOfMajor / 2) {
            resetReplicationStates();
            changeToRole(new LeaderNodeRole(role.getTerm(), scheduleLogReplicationTask()));
            context.log().appendEntry(role.getTerm()); // no-op log
            context.connector().resetChannels();
        } else {
            changeToRole(new CandidateNodeRole(role.getTerm(), currentVotesCount, scheduleElectionTimeout()));
        }
    }

    @Subscribe
    public void onReceiveAppendEntriesRpc(AppendEntriesRpcMessage rpcMessage) {
        context.taskExecutor().submit(() ->
                        context.connector().replyAppendEntries(doProcessAppendEntriesRpc(rpcMessage), rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private AppendEntriesResult doProcessAppendEntriesRpc(AppendEntriesRpcMessage rpcMessage) {
        // leader id is not set when node starts
        // so it's ok not to check if source node id is current leader id
        AppendEntriesRpc rpc = rpcMessage.get();
        if (rpc.getTerm() < role.getTerm()) {
            return new AppendEntriesResult(rpc.getMessageId(), role.getTerm(), false);
        }
        if (rpc.getTerm() > role.getTerm()) {
            becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), true);
            return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), appendEntries(rpc));
        }
        assert rpc.getTerm() == role.getTerm();
        switch (role.getName()) {
            case FOLLOWER:
                becomeFollower(rpc.getTerm(), ((FollowerNodeRole) role).getVotedFor(), rpc.getLeaderId(), true);
                return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), appendEntries(rpc));
            case CANDIDATE:
                // more than 1 candidate but another node win the election
                becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), true);
                return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), appendEntries(rpc));
            case LEADER:
                logger.warn("receive append entries rpc from another leader {}, ignore", rpc.getLeaderId());
                return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), false);
            default:
                throw new IllegalStateException("unexpected node role [" + role.getName() + "]");
        }
    }

    private boolean appendEntries(AppendEntriesRpc rpc) {
        boolean result = context.log().appendEntriesFromLeader(rpc.getPrevLogIndex(), rpc.getPrevLogTerm(), rpc.getEntries());
        if (result) {
            context.log().advanceCommitIndex(Math.min(rpc.getLeaderCommit(), rpc.getLastEntryIndex()), rpc.getTerm());
        }
        return result;
    }

    @Subscribe
    public void onReceiveAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
        context.taskExecutor().submit(() -> doProcessAppendEntriesResult(resultMessage), LOGGING_FUTURE_CALLBACK);
    }

    Future<?> processAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
        return context.taskExecutor().submit(() -> doProcessAppendEntriesResult(resultMessage));
    }

    private void doProcessAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
        AppendEntriesResult result = resultMessage.get();
        if (result.getTerm() > role.getTerm()) {
            becomeFollower(result.getTerm(), null, null, true);
            return;
        }
        // new node
        if (newNodeCatchUpTaskGroup.onReceiveAppendEntriesResult(resultMessage, context.log().getNextIndex())) {
            return;
        }
        NodeId sourceNodeId = resultMessage.getSourceNodeId();
        NodeGroup.NodeState nodeState = context.group().getState(sourceNodeId);
        if (nodeState == null) {
            logger.info("unexpected append entries result from node {}, node maybe removed", sourceNodeId);
            return;
        }
        ReplicatingState replicatingState = nodeState.getReplicatingState();
        AppendEntriesRpc rpc = resultMessage.getRpc();
        if (result.isSuccess()) {
            if (nodeState.isMemberOfMajor()) {
                // peer
                if (replicatingState.advance(rpc.getLastEntryIndex())) {
                    context.log().advanceCommitIndex(context.group().getMatchIndexOfMajor(), role.getTerm());
                }
                if (replicatingState.catchUp(context.log().getNextIndex())) {
                    replicatingState.stopReplicating();
                    return;
                }
            } else {
                // removing node
                if (nodeState.isRemoving()) {
                    logger.debug("node {} is removing, skip", sourceNodeId);
                } else {
                    logger.warn("unexpected append entries result from node {}, not major and not removing", sourceNodeId);
                }
                replicatingState.stopReplicating();
                return;
            }
        } else {
            if (!replicatingState.backOffNextIndex()) {
                logger.warn("cannot back off next index more, node {}", sourceNodeId);
                replicatingState.stopReplicating();
                return;
            }
        }
        doReplicateLog(replicatingState, context.config().getMaxReplicationEntries());
    }

    @Subscribe
    public void onReceiveInstallSnapshotRpc(InstallSnapshotRpcMessage rpcMessage) {
        context.taskExecutor().submit(
                () -> context.connector().replyInstallSnapshot(doProcessInstallSnapshotRpc(rpcMessage), rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private InstallSnapshotResult doProcessInstallSnapshotRpc(InstallSnapshotRpcMessage rpcMessage) {
        InstallSnapshotRpc rpc = rpcMessage.get();
        if (rpc.getTerm() < role.getTerm()) {
            return new InstallSnapshotResult(role.getTerm());
        }
        if (rpc.getTerm() > role.getTerm()) {
            becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), true);
        }
        context.log().installSnapshot(rpc);
        return new InstallSnapshotResult(rpc.getTerm());
    }

    @Subscribe
    public void onReceiveInstallSnapshotResult(InstallSnapshotResultMessage resultMessage) {
        context.taskExecutor().submit(
                () -> doProcessInstallSnapshotResult(resultMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private void doProcessInstallSnapshotResult(InstallSnapshotResultMessage resultMessage) {
        InstallSnapshotResult result = resultMessage.get();
        if (result.getTerm() > role.getTerm()) {
            becomeFollower(result.getTerm(), null, null, true);
            return;
        }
        InstallSnapshotRpc rpc = resultMessage.getRpc();
        NodeId sourceNodeId = resultMessage.getSourceNodeId();
        if (rpc.isDone()) {
            NodeGroup.NodeState nodeState = context.group().getState(sourceNodeId);
            ReplicatingState replicatingState = nodeState.getReplicatingState();
            replicatingState.advance(rpc.getLastIncludedIndex());
            int maxEntries = nodeState.isMemberOfMajor() ? context.config().getMaxReplicationEntries() : context.config().getMaxReplicationEntriesForNewNode();
            doReplicateLog(replicatingState, maxEntries);
        } else {
            InstallSnapshotRpc nextRpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(),
                    rpc.getOffset() + rpc.getDataLength(), context.config().getSnapshotDataLength());
            context.connector().sendInstallSnapshot(nextRpc, sourceNodeId);
        }
    }

    @Subscribe
    public void onGroupConfigEntryFromLeaderAppend(GroupConfigEntryFromLeaderAppendEvent event) {
        context.taskExecutor().submit(() -> {
            GroupConfigEntry entry = event.getEntry();
            context.group().updateNodes(entry.getResultNodeEndpoints());
        }, LOGGING_FUTURE_CALLBACK);
    }

    @Subscribe
    public void onGroupConfigEntryCommitted(GroupConfigEntryCommittedEvent event) {
        context.taskExecutor().submit(
                () -> doProcessGroupConfigEntryCommittedEvent(event),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private void doProcessGroupConfigEntryCommittedEvent(GroupConfigEntryCommittedEvent event) {
        GroupConfigEntry entry = event.getEntry();
        groupConfigChangeTaskHolder.onLogCommitted(entry);
        // group config was applied when append except leader
    }

    @Subscribe
    public void onGroupConfigEntryBatchRemoved(GroupConfigEntryBatchRemovedEvent event) {
        context.taskExecutor().submit(() -> {
            GroupConfigEntry entry = event.getFirstRemovedEntry();
            context.group().updateNodes(entry.getNodeEndpoints());
        }, LOGGING_FUTURE_CALLBACK);
    }

    @Subscribe
    public void onReceiveDeadEvent(DeadEvent deadEvent) {
        logger.warn("dead event {}", deadEvent);
    }

    @Override
    public void stop() throws InterruptedException {
        context.scheduler().stop();
        context.log().close();
        context.connector().close();
        context.store().close();
        context.taskExecutor().shutdown();
        context.groupConfigChangeTaskExecutor().shutdown();
    }

    private class NewNodeCatchUpTaskContextImpl implements NewNodeCatchUpTaskContext {

        @Override
        public void replicateLog(NodeEndpoint endpoint) {
            context.taskExecutor().submit(
                    () -> doReplicateLog(endpoint, context.log().getNextIndex()),
                    LOGGING_FUTURE_CALLBACK
            );
        }

        @Override
        public void doReplicateLog(NodeEndpoint endpoint, int nextIndex) {
            try {
                AppendEntriesRpc rpc = context.log().createAppendEntriesRpc(role.getTerm(), context.selfId(), context.log().getNextIndex(),
                        context.config().getMaxReplicationEntriesForNewNode());
                context.connector().sendAppendEntries(rpc, endpoint);
            } catch (EntryInSnapshotException e) {
                logger.debug("log entry {} in snapshot, replicate with install snapshot RPC", nextIndex);
                InstallSnapshotRpc rpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(), 0, context.config().getSnapshotDataLength());
                context.connector().sendInstallSnapshot(rpc, endpoint);
            }
        }

        @Override
        public void done(NewNodeCatchUpTask task) {
            newNodeCatchUpTaskGroup.remove(task);
        }
    }

    private class GroupConfigChangeTaskContextImpl implements GroupConfigChangeTaskContext {

        @Override
        public void doAddNode(NodeEndpoint endpoint, int nextIndex, int matchIndex) {
            context.log().appendEntryForAddNode(role.getTerm(), context.group().getNodeEndpointsOfMajor(), endpoint);
            assert !context.selfId().equals(endpoint.getId());
            context.group().addNode(endpoint, nextIndex, matchIndex, true);
            NodeImpl.this.doReplicateLog();
        }

        @Override
        public void downgradeNode(NodeId nodeId) {
            context.taskExecutor().submit(() -> {
                context.group().downgrade(nodeId);
                Set<NodeEndpoint> nodeEndpoints = context.group().getNodeEndpointsOfMajor();
                context.log().appendEntryForRemoveNode(role.getTerm(), nodeEndpoints, nodeId);
                NodeImpl.this.doReplicateLog();
            }, LOGGING_FUTURE_CALLBACK);
        }

        @Override
        public void removeNode(NodeId nodeId) {
            context.taskExecutor().submit(() -> {
                if (nodeId.equals(context.selfId())) {
                    logger.info("remove self from group, step down and standby");
                    becomeFollower(role.getTerm(), null, null, false);
                }
                context.group().removeNode(nodeId);
            }, LOGGING_FUTURE_CALLBACK);
        }

        @Override
        public void done() {
            synchronized (NodeImpl.this) {
                groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder();
            }
        }

    }

}
