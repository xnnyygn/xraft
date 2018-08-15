package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import in.xnnyygn.xraft.core.log.*;
import in.xnnyygn.xraft.core.log.entry.*;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryBatchRemovedEvent;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryCommittedEvent;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryFromLeaderAppendEvent;
import in.xnnyygn.xraft.core.log.replication.NewNodeReplicatingState;
import in.xnnyygn.xraft.core.log.replication.ReplicatingState;
import in.xnnyygn.xraft.core.log.snapshot.EntryInSnapshotException;
import in.xnnyygn.xraft.core.noderole.*;
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
    private final TaskReferenceHolder taskReferenceHolder = new TaskReferenceHolder();


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
    public TaskReference addServer(NodeEndpoint newNodeEndpoint) {
        ensureLeader();
        AddNodeTaskReference taskReference = new AddNodeTaskReference(newNodeEndpoint.getId());
        context.taskExecutor().submit(() -> {
            GroupConfigEntry lastUncommittedGroupConfigEntry = context.log().getLastUncommittedGroupConfigEntry();
            if (!taskReferenceHolder.awaitPreviousGroupConfigChange(
                    lastUncommittedGroupConfigEntry, taskReference,
                    context.config().getPreviousGroupConfigChangeTimeout())) {
                logger.info("previous group config change cannot complete with specified timeout, skip adding server");
                return;
            }
            context.group().addNode(newNodeEndpoint, context.log().getNextIndex(), false);
            doReplicateLog(newNodeEndpoint.getId(), context.config().getMaxReplicationEntriesForNewNode());
            taskReferenceHolder.set(taskReference);
        }, LOGGING_FUTURE_CALLBACK);
        return taskReference;
    }

    private void ensureLeader() {
        RoleNameAndLeaderId roleNameAndLeaderId = role.getNameAndLeaderId(context.selfId());
        if (roleNameAndLeaderId.getRoleName() != RoleName.LEADER) {
            throw new NotLeaderException(roleNameAndLeaderId.getRoleName(), roleNameAndLeaderId.getLeaderId());
        }
    }

    @Override
    public TaskReference removeServer(NodeId id) {
        ensureLeader();
        EntryTaskReference taskReference = new EntryTaskReference();
        context.taskExecutor().submit(() -> {
            GroupConfigEntry lastUncommittedGroupConfigEntry = context.log().getLastUncommittedGroupConfigEntry();
            if (!taskReferenceHolder.awaitPreviousGroupConfigChange(
                    lastUncommittedGroupConfigEntry, taskReference,
                    context.config().getPreviousGroupConfigChangeTimeout())) {
                logger.info("previous group config change cannot complete with specified timeout, skip removing server");
                return;
            }
            Set<NodeEndpoint> nodeEndpoints = context.group().getNodeEndpointsOfMajor();
            RemoveNodeEntry entry = context.log().appendEntryForRemoveNode(role.getTerm(), nodeEndpoints, id);
            taskReference.setEntryIndex(entry.getIndex());
            taskReferenceHolder.set(taskReference);
            context.group().downgrade(id);
            doReplicateLog();
        }, LOGGING_FUTURE_CALLBACK);
        return taskReference;
    }

    /**
     * event: election timeout
     */
    void electionTimeout() {
        context.taskExecutor().submit(this::processElectionTimeout, LOGGING_FUTURE_CALLBACK);
    }

    private void processElectionTimeout() {
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
                        context.connector().replyRequestVote(processRequestVoteRpc(rpcMessage), rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private RequestVoteResult processRequestVoteRpc(RequestVoteRpcMessage rpcMessage) {
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

    @Subscribe
    public void onReceiveRequestVoteResult(RequestVoteResult result) {
        context.taskExecutor().submit(() -> processRequestVoteResult(result), LOGGING_FUTURE_CALLBACK);
    }

    private void processRequestVoteResult(RequestVoteResult result) {
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
                        context.connector().replyAppendEntries(processAppendEntriesRpc(rpcMessage), rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private AppendEntriesResult processAppendEntriesRpc(AppendEntriesRpcMessage rpcMessage) {
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
        context.taskExecutor().submit(() -> processAppendEntriesResult(resultMessage));
    }

    private void processAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
        AppendEntriesResult result = resultMessage.get();
        if (result.getTerm() > role.getTerm()) {
            becomeFollower(result.getTerm(), null, null, true);
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
        int maxEntries = context.config().getMaxReplicationEntries();
        // TODO refactor
        if (result.isSuccess()) {
            if (nodeState.isMemberOfMajor()) { // peer
                // peer
                if (replicatingState.advance(rpc.getLastEntryIndex())) {
                    context.log().advanceCommitIndex(context.group().getMatchIndexOfMajor(), role.getTerm());
                }
                if (replicatingState.catchUp(context.log().getNextIndex())) {
                    replicatingState.stopReplicating();
                    return;
                }
            } else { // new node
                if (nodeState.isRemoving()) {
                    logger.debug("node {} is removing, skip", sourceNodeId);
                    replicatingState.stopReplicating();
                    return;
                }
                logger.debug("replication state of new node, {}", replicatingState);
                replicatingState.advance(rpc.getLastEntryIndex());
                if (replicatingState.catchUp(context.log().getNextIndex())) {
                    NodeEndpoint newNodeEndpoint = context.group().findEndpoint(sourceNodeId);
                    AddNodeEntry entry = context.log().appendEntryForAddNode(role.getTerm(), context.group().getNodeEndpointsOfMajor(), newNodeEndpoint);
                    context.group().upgrade(sourceNodeId);
                    taskReferenceHolder.setEntryIndexForAddNode(entry.getIndex());
                    replicatingState.stopReplicating();
                    return;
                }

                NewNodeReplicatingState newNodeReplicationState = (NewNodeReplicatingState) replicatingState;
                newNodeReplicationState.increaseRound();
                if (newNodeReplicationState.roundExceedOrTimeout(context.config().getNewNodeMaxRound(),
                        context.config().getNewNodeTimeout())) {
                    logger.info("node {} cannot catch up within max round and timeout", sourceNodeId);
                    context.group().removeNode(sourceNodeId);
                    taskReferenceHolder.doneForAddNode(TaskReference.Result.TIMEOUT);
                    return;
                }
                maxEntries = context.config().getMaxReplicationEntriesForNewNode();
            }
        } else {
            // TODO handle new node
            // TODO handle timeout
            if (!replicatingState.backOffNextIndex()) {
                logger.warn("cannot back off next index more, node {}", sourceNodeId);
                replicatingState.stopReplicating();
                return;
            }
        }
        doReplicateLog(replicatingState, maxEntries);
    }

    @Subscribe
    public void onReceiveInstallSnapshotRpc(InstallSnapshotRpcMessage rpcMessage) {
        context.taskExecutor().submit(
                () -> context.connector().replyInstallSnapshot(processInstallSnapshotRpc(rpcMessage), rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private InstallSnapshotResult processInstallSnapshotRpc(InstallSnapshotRpcMessage rpcMessage) {
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
                () -> processInstallSnapshotResult(resultMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private void processInstallSnapshotResult(InstallSnapshotResultMessage resultMessage) {
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
            context.group().updateNodes(entry.getResultNodeConfigs());
        }, LOGGING_FUTURE_CALLBACK);
    }

    @Subscribe
    public void onGroupConfigEntryCommitted(GroupConfigEntryCommittedEvent event) {
        context.taskExecutor().submit(
                () -> processGroupConfigEntryCommittedEvent(event),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private void processGroupConfigEntryCommittedEvent(GroupConfigEntryCommittedEvent event) {
        GroupConfigEntry entry = event.getEntry();
        if (entry.getKind() == Entry.KIND_REMOVE_NODE) {
            RemoveNodeEntry removeNodeEntry = (RemoveNodeEntry) entry;
            NodeId nodeToRemove = removeNodeEntry.getNodeToRemove();
            if (nodeToRemove.equals(context.selfId())) {
                logger.info("remove leader from group, step down");
                becomeFollower(role.getTerm(), null, null, false);
            }
            context.group().removeNode(nodeToRemove);
        } else if (entry.getKind() == Entry.KIND_ADD_NODE) {
            AddNodeEntry addNodeEntry = (AddNodeEntry) entry;
            logger.info("node {} added", addNodeEntry.getNewNodeEndpoint().getId());
        }
        taskReferenceHolder.done(entry.getIndex());
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
    }

}
