package in.xnnyygn.xraft.core.node;

import com.google.common.base.Preconditions;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import in.xnnyygn.xraft.core.log.StateMachine;
import in.xnnyygn.xraft.core.log.entry.EntryMeta;
import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryBatchRemovedEvent;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryCommittedEvent;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryFromLeaderAppendEvent;
import in.xnnyygn.xraft.core.log.snapshot.EntryInSnapshotException;
import in.xnnyygn.xraft.core.node.role.*;
import in.xnnyygn.xraft.core.node.store.NodeStore;
import in.xnnyygn.xraft.core.node.task.*;
import in.xnnyygn.xraft.core.rpc.message.*;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * Node implementation.
 *
 * @see NodeContext
 */
@ThreadSafe
public class NodeImpl implements Node {

    private static final Logger logger = LoggerFactory.getLogger(NodeImpl.class);

    // callback for async tasks.
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
    @GuardedBy("this")
    private boolean started;
    private volatile AbstractNodeRole role;
    private final List<NodeRoleListener> roleListeners = new CopyOnWriteArrayList<>();

    // NewNodeCatchUpTask and GroupConfigChangeTask related
    private final NewNodeCatchUpTaskContext newNodeCatchUpTaskContext = new NewNodeCatchUpTaskContextImpl();
    private final NewNodeCatchUpTaskGroup newNodeCatchUpTaskGroup = new NewNodeCatchUpTaskGroup();
    private final GroupConfigChangeTaskContext groupConfigChangeTaskContext = new GroupConfigChangeTaskContextImpl();
    private volatile GroupConfigChangeTaskHolder groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder();

    /**
     * Create with context.
     *
     * @param context context
     */
    NodeImpl(NodeContext context) {
        this.context = context;
    }

    /**
     * Get context.
     *
     * @return context
     */
    NodeContext getContext() {
        return context;
    }

    @Override
    public synchronized void registerStateMachine(@Nonnull StateMachine stateMachine) {
        Preconditions.checkNotNull(stateMachine);
        context.log().setStateMachine(stateMachine);
    }

    @Override
    @Nonnull
    public RoleNameAndLeaderId getRoleNameAndLeaderId() {
        return role.getNameAndLeaderId(context.selfId());
    }

    /**
     * Get role state.
     *
     * @return role state
     */
    @Nonnull
    RoleState getRoleState() {
        return role.getState();
    }

    @Override
    public void addNodeRoleListener(@Nonnull NodeRoleListener listener) {
        Preconditions.checkNotNull(listener);
        roleListeners.add(listener);
    }

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }
        logger.info("start node {}", context.selfId());
        context.eventBus().register(this);
        context.connector().initialize();

        // load term, votedFor from store and become follower
        NodeStore store = context.store();
        changeToRole(new FollowerNodeRole(store.getTerm(), store.getVotedFor(), null, scheduleElectionTimeout()));
        started = true;
    }

    @Override
    public void appendLog(@Nonnull byte[] commandBytes) {
        Preconditions.checkNotNull(commandBytes);
        ensureLeader();
        context.taskExecutor().submit(() -> {
            context.log().appendEntry(role.getTerm(), commandBytes);
            doReplicateLog();
        }, LOGGING_FUTURE_CALLBACK);
    }

    @Override
    @Nonnull
    public GroupConfigChangeTaskReference addNode(@Nonnull NodeEndpoint endpoint) {
        Preconditions.checkNotNull(endpoint);
        ensureLeader();

        // self cannot be added
        if (context.selfId().equals(endpoint.getId())) {
            throw new IllegalArgumentException("new node cannot be self");
        }

        NewNodeCatchUpTask newNodeCatchUpTask = new NewNodeCatchUpTask(newNodeCatchUpTaskContext, endpoint, context.config());

        // task for node exists
        if (!newNodeCatchUpTaskGroup.add(newNodeCatchUpTask)) {
            throw new IllegalArgumentException("node " + endpoint.getId() + " is adding");
        }

        // catch up new server
        // this will be run in caller thread
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
            if (!(e instanceof InterruptedException)) {
                logger.warn("failed to catch up new node " + endpoint.getId(), e);
            }
            return new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.ERROR);
        }

        // new server caught up
        // wait for previous group config change
        // it will wait forever by default, but you can change to fixed timeout by setting in NodeConfig
        GroupConfigChangeTaskResult result = awaitPreviousGroupConfigChangeTask();
        if (result != null) {
            return new FixedResultGroupConfigTaskReference(result);
        }

        // submit group config change task
        synchronized (this) {

            // it will happen when try to add two or more nodes at the same time
            if (!groupConfigChangeTaskHolder.isEmpty()) {
                throw new IllegalStateException("group config change concurrently");
            }

            AddNodeTask addNodeTask = new AddNodeTask(groupConfigChangeTaskContext, endpoint, newNodeCatchUpTaskResult);
            Future<GroupConfigChangeTaskResult> future = context.groupConfigChangeTaskExecutor().submit(addNodeTask);
            GroupConfigChangeTaskReference reference = new FutureGroupConfigChangeTaskReference(future);
            groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder(addNodeTask, reference);
            return reference;
        }
    }

    /**
     * Await previous group config change task.
     *
     * @return {@code null} if previous task done, otherwise error or timeout
     * @see GroupConfigChangeTaskResult#ERROR
     * @see GroupConfigChangeTaskResult#TIMEOUT
     */
    @Nullable
    private GroupConfigChangeTaskResult awaitPreviousGroupConfigChangeTask() {
        try {
            groupConfigChangeTaskHolder.awaitDone(context.config().getPreviousGroupConfigChangeTimeout());
            return null;
        } catch (InterruptedException ignored) {
            return GroupConfigChangeTaskResult.ERROR;
        } catch (TimeoutException ignored) {
            logger.info("previous cannot complete within timeout");
            return GroupConfigChangeTaskResult.TIMEOUT;
        }
    }

    /**
     * Ensure leader status
     *
     * @throws NotLeaderException if not leader
     */
    private void ensureLeader() {
        RoleNameAndLeaderId roleNameAndLeaderId = role.getNameAndLeaderId(context.selfId());
        if (roleNameAndLeaderId.getRoleName() != RoleName.LEADER) {
            throw new NotLeaderException(roleNameAndLeaderId.getRoleName(), roleNameAndLeaderId.getLeaderId());
        }
    }

    @Override
    @Nonnull
    public GroupConfigChangeTaskReference removeNode(@Nonnull NodeId id) {
        Preconditions.checkNotNull(id);
        ensureLeader();

        // await previous group config change task
        GroupConfigChangeTaskResult result = awaitPreviousGroupConfigChangeTask();
        if (result != null) {
            return new FixedResultGroupConfigTaskReference(result);
        }

        // submit group config change task
        synchronized (this) {

            // it will happen when try to remove two or more nodes at the same time
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

    /**
     * Cancel current group config change task
     */
    synchronized void cancelGroupConfigChangeTask() {
        if (groupConfigChangeTaskHolder.isEmpty()) {
            return;
        }
        logger.info("cancel group config change task");
        groupConfigChangeTaskHolder.cancel();
        groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder();
    }

    /**
     * Election timeout
     * <p>
     * Source: scheduler
     * </p>
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

                // become leader
                logger.info("no other node, just become leader");
                resetReplicatingStates();
                changeToRole(new LeaderNodeRole(newTerm, scheduleLogReplicationTask()));
                context.log().appendEntry(newTerm); // no-op log
            }
        } else {
            changeToRole(new CandidateNodeRole(newTerm, scheduleElectionTimeout()));

            // request vote
            EntryMeta lastEntryMeta = context.log().getLastEntryMeta();
            RequestVoteRpc rpc = new RequestVoteRpc();
            rpc.setTerm(newTerm);
            rpc.setCandidateId(context.selfId());
            rpc.setLastLogIndex(lastEntryMeta.getIndex());
            rpc.setLastLogTerm(lastEntryMeta.getTerm());
            context.connector().sendRequestVote(rpc, context.group().listEndpointOfMajorExcept(context.selfId()));
        }
    }

    /**
     * Become follower.
     *
     * @param term                    term
     * @param votedFor                voted for
     * @param leaderId                leader id
     * @param scheduleElectionTimeout schedule election timeout or not
     */
    private void becomeFollower(int term, NodeId votedFor, NodeId leaderId, boolean scheduleElectionTimeout) {
        role.cancelTimeoutOrTask();
        ElectionTimeout electionTimeout = scheduleElectionTimeout ? scheduleElectionTimeout() : ElectionTimeout.NONE;
        changeToRole(new FollowerNodeRole(term, votedFor, leaderId, electionTimeout));
    }

    /**
     * Change role.
     *
     * @param newRole new role
     */
    private void changeToRole(AbstractNodeRole newRole) {
        if (!isStableBetween(role, newRole)) {
            logger.debug("node {}, role state changed -> {}", context.selfId(), newRole);
            RoleState state = newRole.getState();

            // update store
            NodeStore store = context.store();
            store.setTerm(state.getTerm());
            store.setVotedFor(state.getVotedFor());

            // notify listeners
            roleListeners.forEach(l -> l.nodeRoleChanged(state));
        }
        role = newRole;
    }

    /**
     * Check if stable between two roles.
     * <p>
     * It is stable when role name not changed and role state except timeout/task not change.
     * </p>
     * <p>
     * If role state except timeout/task not changed, it should not update store or notify listeners.
     * </p>
     *
     * @param before role before
     * @param after  role after
     * @return true if stable, otherwise false
     * @see AbstractNodeRole#stateEquals(AbstractNodeRole)
     */
    // TODO add test
    boolean isStableBetween(AbstractNodeRole before, AbstractNodeRole after) {
        assert after != null;
        return before != null && before.stateEquals(after);
    }

    /**
     * Schedule election timeout.
     *
     * @return election timeout
     */
    private ElectionTimeout scheduleElectionTimeout() {
        return context.scheduler().scheduleElectionTimeout(this::electionTimeout);
    }

    /**
     * Reset replicating states.
     */
    private void resetReplicatingStates() {
        context.group().resetReplicatingStates(context.selfId(), context.log());
    }

    /**
     * Schedule log replication task.
     *
     * @return log replication task
     */
    private LogReplicationTask scheduleLogReplicationTask() {
        return context.scheduler().scheduleLogReplicationTask(this::replicateLog);
    }

    /**
     * Replicate log.
     * <p>
     * Source: scheduler.
     * </p>
     */
    void replicateLog() {
        context.taskExecutor().submit(this::doReplicateLog, LOGGING_FUTURE_CALLBACK);
    }

    /**
     * Replicate log to other nodes.
     */
    private void doReplicateLog() {
        // just advance commit index if is unique node
        if (context.group().isUniqueNode(context.selfId())) {
            context.log().advanceCommitIndex(context.log().getNextIndex() - 1, role.getTerm());
            return;
        }
        logger.debug("replicate log");
        for (GroupMember member : context.group().listReplicationTarget()) {
            if (member.shouldReplicate(context.config().getMinReplicationInterval())) {
                doReplicateLog(member, context.config().getMaxReplicationEntries());
            } else {
                logger.debug("node {} is replicating, skip replication task", member.getId());
            }
        }
    }

    /**
     * Replicate log to specified node.
     * <p>
     * Normally it will send append entries rpc to node. And change to install snapshot rpc if entry in snapshot.
     * </p>
     *
     * @param member     node
     * @param maxEntries max entries
     * @see EntryInSnapshotException
     */
    private void doReplicateLog(GroupMember member, int maxEntries) {
        member.startReplicating();
        try {
            AppendEntriesRpc rpc = context.log().createAppendEntriesRpc(role.getTerm(), context.selfId(), member.getNextIndex(), maxEntries);
            context.connector().sendAppendEntries(rpc, member.getEndpoint());
        } catch (EntryInSnapshotException ignored) {
            logger.debug("log entry {} in snapshot, replicate with install snapshot RPC", member.getNextIndex());
            InstallSnapshotRpc rpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(), 0, context.config().getSnapshotDataLength());
            context.connector().sendInstallSnapshot(rpc, member.getEndpoint());
        }
    }

    /**
     * Receive request vote rpc.
     * <p>
     * Source: connector.
     * </p>
     *
     * @param rpcMessage rpc message
     */
    @Subscribe
    public void onReceiveRequestVoteRpc(RequestVoteRpcMessage rpcMessage) {
        context.taskExecutor().submit(
                () -> context.connector().replyRequestVote(doProcessRequestVoteRpc(rpcMessage), rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private RequestVoteResult doProcessRequestVoteRpc(RequestVoteRpcMessage rpcMessage) {

        // skip non-major node, it maybe removed node
        if (!context.group().isMemberOfMajor(rpcMessage.getSourceNodeId())) {
            logger.warn("receive request vote rpc from node {} which is not major node, ignore", rpcMessage.getSourceNodeId());
            return new RequestVoteResult(role.getTerm(), false);
        }

        // reply current term if result's term is smaller than current one
        RequestVoteRpc rpc = rpcMessage.get();
        if (rpc.getTerm() < role.getTerm()) {
            logger.debug("term from rpc < current term, don't vote ({} < {})", rpc.getTerm(), role.getTerm());
            return new RequestVoteResult(role.getTerm(), false);
        }

        // step down if result's term is larger than current term
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
                // reply vote granted for
                // 1. not voted and candidate's log is newer than self
                // 2. voted for candidate
                if ((votedFor == null && !context.log().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm())) ||
                        Objects.equals(votedFor, rpc.getCandidateId())) {
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

    /**
     * Receive request vote result.
     * <p>
     * Source: connector.
     * </p>
     *
     * @param result result
     */
    @Subscribe
    public void onReceiveRequestVoteResult(RequestVoteResult result) {
        context.taskExecutor().submit(() -> doProcessRequestVoteResult(result), LOGGING_FUTURE_CALLBACK);
    }

    Future<?> processRequestVoteResult(RequestVoteResult result) {
        return context.taskExecutor().submit(() -> doProcessRequestVoteResult(result));
    }

    private void doProcessRequestVoteResult(RequestVoteResult result) {

        // step down if result's term is larger than current term
        if (result.getTerm() > role.getTerm()) {
            becomeFollower(result.getTerm(), null, null, true);
            return;
        }

        // check role
        if (role.getName() != RoleName.CANDIDATE) {
            logger.debug("receive request vote result and current role is not candidate, ignore");
            return;
        }

        // do nothing if not vote granted
        if (!result.isVoteGranted()) {
            return;
        }

        int currentVotesCount = ((CandidateNodeRole) role).getVotesCount() + 1;
        int countOfMajor = context.group().getCountOfMajor();
        logger.debug("votes count {}, major node count {}", currentVotesCount, countOfMajor);
        role.cancelTimeoutOrTask();
        if (currentVotesCount > countOfMajor / 2) {

            // become leader
            resetReplicatingStates();
            changeToRole(new LeaderNodeRole(role.getTerm(), scheduleLogReplicationTask()));
            context.log().appendEntry(role.getTerm()); // no-op log
            context.connector().resetChannels(); // close all inbound channels
        } else {

            // update votes count
            changeToRole(new CandidateNodeRole(role.getTerm(), currentVotesCount, scheduleElectionTimeout()));
        }
    }

    /**
     * Receive append entries rpc.
     * <p>
     * Source: connector.
     * </p>
     *
     * @param rpcMessage rpc message
     */
    @Subscribe
    public void onReceiveAppendEntriesRpc(AppendEntriesRpcMessage rpcMessage) {
        context.taskExecutor().submit(() ->
                        context.connector().replyAppendEntries(doProcessAppendEntriesRpc(rpcMessage), rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private AppendEntriesResult doProcessAppendEntriesRpc(AppendEntriesRpcMessage rpcMessage) {
        AppendEntriesRpc rpc = rpcMessage.get();

        // reply current term if term in rpc is smaller than current term
        if (rpc.getTerm() < role.getTerm()) {
            return new AppendEntriesResult(rpc.getMessageId(), role.getTerm(), false);
        }

        // if term in rpc is larger than current term, step down and append entries
        if (rpc.getTerm() > role.getTerm()) {
            becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), true);
            return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), appendEntries(rpc));
        }

        assert rpc.getTerm() == role.getTerm();
        switch (role.getName()) {
            case FOLLOWER:

                // reset election timeout and append entries
                becomeFollower(rpc.getTerm(), ((FollowerNodeRole) role).getVotedFor(), rpc.getLeaderId(), true);
                return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), appendEntries(rpc));
            case CANDIDATE:

                // more than one candidate but another node won the election
                becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), true);
                return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), appendEntries(rpc));
            case LEADER:
                logger.warn("receive append entries rpc from another leader {}, ignore", rpc.getLeaderId());
                return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), false);
            default:
                throw new IllegalStateException("unexpected node role [" + role.getName() + "]");
        }
    }

    /**
     * Append entries and advance commit index if possible.
     *
     * @param rpc rpc
     * @return {@code true} if log appended, {@code false} if previous log check failed, etc
     */
    private boolean appendEntries(AppendEntriesRpc rpc) {
        boolean result = context.log().appendEntriesFromLeader(rpc.getPrevLogIndex(), rpc.getPrevLogTerm(), rpc.getEntries());
        if (result) {
            context.log().advanceCommitIndex(Math.min(rpc.getLeaderCommit(), rpc.getLastEntryIndex()), rpc.getTerm());
        }
        return result;
    }

    /**
     * Receive append entries result.
     *
     * @param resultMessage result message
     */
    @Subscribe
    public void onReceiveAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
        context.taskExecutor().submit(() -> doProcessAppendEntriesResult(resultMessage), LOGGING_FUTURE_CALLBACK);
    }

    Future<?> processAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
        return context.taskExecutor().submit(() -> doProcessAppendEntriesResult(resultMessage));
    }

    private void doProcessAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
        AppendEntriesResult result = resultMessage.get();

        // step down if result's term is larger than current term
        if (result.getTerm() > role.getTerm()) {
            becomeFollower(result.getTerm(), null, null, true);
            return;
        }

        // dispatch to new node catch up task by node id
        if (newNodeCatchUpTaskGroup.onReceiveAppendEntriesResult(resultMessage, context.log().getNextIndex())) {
            return;
        }

        NodeId sourceNodeId = resultMessage.getSourceNodeId();
        GroupMember member = context.group().getMember(sourceNodeId);
        if (member == null) {
            logger.info("unexpected append entries result from node {}, node maybe removed", sourceNodeId);
            return;
        }
        AppendEntriesRpc rpc = resultMessage.getRpc();
        if (result.isSuccess()) {
            if (member.isMajor()) { // peer

                // advance commit index if major of match index changed
                if (member.advanceReplicatingState(rpc.getLastEntryIndex())) {
                    context.log().advanceCommitIndex(context.group().getMatchIndexOfMajor(), role.getTerm());
                }

                // node caught up
                if (member.getNextIndex() >= context.log().getNextIndex()) {
                    member.stopReplicating();
                    return;
                }
            } else { // removing node
                if (member.isRemoving()) {
                    logger.debug("node {} is removing, skip", sourceNodeId);
                } else {
                    logger.warn("unexpected append entries result from node {}, not major and not removing", sourceNodeId);
                }
                member.stopReplicating();
                return;
            }
        } else {

            // backoff next index if failed to append entries
            if (!member.backOffNextIndex()) {
                logger.warn("cannot back off next index more, node {}", sourceNodeId);
                member.stopReplicating();
                return;
            }
        }

        // replicate log to node immediately other than wait for next log replication
        doReplicateLog(member, context.config().getMaxReplicationEntries());
    }

    /**
     * Receive install snapshot rpc.
     *
     * @param rpcMessage rpc message
     */
    @Subscribe
    public void onReceiveInstallSnapshotRpc(InstallSnapshotRpcMessage rpcMessage) {
        context.taskExecutor().submit(
                () -> context.connector().replyInstallSnapshot(doProcessInstallSnapshotRpc(rpcMessage), rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private InstallSnapshotResult doProcessInstallSnapshotRpc(InstallSnapshotRpcMessage rpcMessage) {
        InstallSnapshotRpc rpc = rpcMessage.get();

        // reply current term if term in rpc is smaller than current term
        if (rpc.getTerm() < role.getTerm()) {
            return new InstallSnapshotResult(role.getTerm());
        }

        // step down if term in rpc is larger than current one
        if (rpc.getTerm() > role.getTerm()) {
            becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), true);
        }
        context.log().installSnapshot(rpc);
        // TODO role check?
        return new InstallSnapshotResult(rpc.getTerm());
    }

    /**
     * Receive install snapshot result.
     *
     * @param resultMessage result message
     */
    @Subscribe
    public void onReceiveInstallSnapshotResult(InstallSnapshotResultMessage resultMessage) {
        context.taskExecutor().submit(
                () -> doProcessInstallSnapshotResult(resultMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private void doProcessInstallSnapshotResult(InstallSnapshotResultMessage resultMessage) {
        InstallSnapshotResult result = resultMessage.get();

        // step down if result's term is larger than current one
        if (result.getTerm() > role.getTerm()) {
            becomeFollower(result.getTerm(), null, null, true);
            return;
        }

        InstallSnapshotRpc rpc = resultMessage.getRpc();
        NodeId sourceNodeId = resultMessage.getSourceNodeId();
        GroupMember member = context.group().getMember(sourceNodeId);
        // TODO add test
        if (member == null) {
            logger.info("unexpected install snapshot result from node {}, node maybe removed", sourceNodeId);
            return;
        }
        if (rpc.isDone()) {

            // change to append entries rpc
            member.advanceReplicatingState(rpc.getLastIncludedIndex());
            int maxEntries = member.isMajor() ? context.config().getMaxReplicationEntries() : context.config().getMaxReplicationEntriesForNewNode();
            doReplicateLog(member, maxEntries);
        } else {

            // transfer data
            InstallSnapshotRpc nextRpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(),
                    rpc.getOffset() + rpc.getDataLength(), context.config().getSnapshotDataLength());
            context.connector().sendInstallSnapshot(nextRpc, member.getEndpoint());
        }
    }

    /**
     * Group config from leader appended.
     * <p>
     * Source: log.
     * </p>
     *
     * @param event event
     */
    @Subscribe
    public void onGroupConfigEntryFromLeaderAppend(GroupConfigEntryFromLeaderAppendEvent event) {
        context.taskExecutor().submit(() -> {
            GroupConfigEntry entry = event.getEntry();
            context.group().updateNodes(entry.getResultNodeEndpoints());
        }, LOGGING_FUTURE_CALLBACK);
    }

    /**
     * Group config entry committed.
     * <p>
     * Source: log.
     * </p>
     *
     * @param event event
     */
    @Subscribe
    public void onGroupConfigEntryCommitted(GroupConfigEntryCommittedEvent event) {
        context.taskExecutor().submit(
                () -> doProcessGroupConfigEntryCommittedEvent(event),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private void doProcessGroupConfigEntryCommittedEvent(GroupConfigEntryCommittedEvent event) {
        GroupConfigEntry entry = event.getEntry();

        // dispatch to group config change task by node id
        groupConfigChangeTaskHolder.onLogCommitted(entry);
    }

    /**
     * Multiple group configs removed.
     * <p>
     * Source: log.
     * </p>
     *
     * @param event event
     */
    @Subscribe
    public void onGroupConfigEntryBatchRemoved(GroupConfigEntryBatchRemovedEvent event) {
        context.taskExecutor().submit(() -> {
            GroupConfigEntry entry = event.getFirstRemovedEntry();
            context.group().updateNodes(entry.getNodeEndpoints());
        }, LOGGING_FUTURE_CALLBACK);
    }

    /**
     * Dead event.
     * <p>
     * Source: event-bus.
     * </p>
     *
     * @param deadEvent dead event
     */
    @Subscribe
    public void onReceiveDeadEvent(DeadEvent deadEvent) {
        logger.warn("dead event {}", deadEvent);
    }

    @Override
    public synchronized void stop() throws InterruptedException {
        if (!started) {
            throw new IllegalStateException("node not started");
        }
        logger.info("stop node {}", context.selfId());
        context.scheduler().stop();
        context.log().close();
        context.connector().close();
        context.store().close();
        context.taskExecutor().shutdown();
        context.groupConfigChangeTaskExecutor().shutdown();
        started = false;
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
            } catch (EntryInSnapshotException ignored) {

                // change to install snapshot rpc if entry in snapshot
                logger.debug("log entry {} in snapshot, replicate with install snapshot RPC", nextIndex);
                InstallSnapshotRpc rpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(), 0, context.config().getSnapshotDataLength());
                context.connector().sendInstallSnapshot(rpc, endpoint);
            }
        }

        @Override
        public void done(NewNodeCatchUpTask task) {

            // remove task from group
            newNodeCatchUpTaskGroup.remove(task);
        }
    }

    private class GroupConfigChangeTaskContextImpl implements GroupConfigChangeTaskContext {

        @Override
        public void addNode(NodeEndpoint endpoint, int nextIndex, int matchIndex) {
            context.taskExecutor().submit(() -> {
                context.log().appendEntryForAddNode(role.getTerm(), context.group().listEndpointOfMajor(), endpoint);
                assert !context.selfId().equals(endpoint.getId());
                context.group().addNode(endpoint, nextIndex, matchIndex, true);
                NodeImpl.this.doReplicateLog();
            }, LOGGING_FUTURE_CALLBACK);
        }

        @Override
        public void downgradeNode(NodeId nodeId) {
            context.taskExecutor().submit(() -> {
                context.group().downgrade(nodeId);
                Set<NodeEndpoint> nodeEndpoints = context.group().listEndpointOfMajor();
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

            // clear current group config change
            synchronized (NodeImpl.this) {
                groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder();
            }
        }

    }

}
