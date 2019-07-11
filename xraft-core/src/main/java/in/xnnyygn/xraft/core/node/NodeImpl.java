package in.xnnyygn.xraft.core.node;

import com.google.common.base.Preconditions;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import in.xnnyygn.xraft.core.log.AppendEntriesState;
import in.xnnyygn.xraft.core.log.InstallSnapshotState;
import in.xnnyygn.xraft.core.log.entry.AddNodeEntry;
import in.xnnyygn.xraft.core.log.entry.EntryMeta;
import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.log.entry.RemoveNodeEntry;
import in.xnnyygn.xraft.core.log.event.SnapshotGeneratedEvent;
import in.xnnyygn.xraft.core.log.snapshot.EntryInSnapshotException;
import in.xnnyygn.xraft.core.log.statemachine.StateMachine;
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
import java.util.*;
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
    private volatile GroupConfigChangeTask currentGroupConfigChangeTask = GroupConfigChangeTask.NONE;
    private volatile GroupConfigChangeTaskReference currentGroupConfigChangeTaskReference
            = new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.OK);

    // Map<RequestId, ReadIndexTask>
    private final Map<String, ReadIndexTask> readIndexTaskMap = new HashMap<>();

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
        context.eventBus().register(this);
        context.connector().initialize();

        Set<NodeEndpoint> lastGroup = context.log().getLastGroup();
        context.group().updateNodes(lastGroup);

        // load term, votedFor from store and become follower
        NodeStore store = context.store();
        changeToRole(new FollowerNodeRole(store.getTerm(), store.getVotedFor(), null, 0, 0, scheduleElectionTimeout()));
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
    public void enqueueReadIndex(@Nonnull String requestId) {
        context.taskExecutor().submit(() -> {
            ReadIndexTask task = new ReadIndexTask(
                    context.log().getCommitIndex(),
                    context.group().listIdOfMajorExceptSelf(),
                    context.log().getStateMachine(),
                    requestId
            );
            logger.debug("enqueue read index task {}", task);
            readIndexTaskMap.put(requestId, task);
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
            if (currentGroupConfigChangeTask != GroupConfigChangeTask.NONE) {
                throw new IllegalStateException("group config change concurrently");
            }

            currentGroupConfigChangeTask = new AddNodeTask(groupConfigChangeTaskContext, endpoint, newNodeCatchUpTaskResult);
            Future<GroupConfigChangeTaskResult> future = context.groupConfigChangeTaskExecutor().submit(currentGroupConfigChangeTask);
            currentGroupConfigChangeTaskReference = new FutureGroupConfigChangeTaskReference(future);
            return currentGroupConfigChangeTaskReference;
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
            currentGroupConfigChangeTaskReference.awaitDone(context.config().getPreviousGroupConfigChangeTimeout());
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
        RoleNameAndLeaderId result = role.getNameAndLeaderId(context.selfId());
        if (result.getRoleName() == RoleName.LEADER) {
            return;
        }
        NodeEndpoint endpoint = result.getLeaderId() != null ? context.group().findMember(result.getLeaderId()).getEndpoint() : null;
        throw new NotLeaderException(result.getRoleName(), endpoint);
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
            if (currentGroupConfigChangeTask != GroupConfigChangeTask.NONE) {
                throw new IllegalStateException("group config change concurrently");
            }

            currentGroupConfigChangeTask = new RemoveNodeTask(groupConfigChangeTaskContext, id, context.selfId());
            Future<GroupConfigChangeTaskResult> future = context.groupConfigChangeTaskExecutor().submit(currentGroupConfigChangeTask);
            currentGroupConfigChangeTaskReference = new FutureGroupConfigChangeTaskReference(future);
            return currentGroupConfigChangeTaskReference;
        }
    }

    /**
     * Cancel current group config change task
     */
    synchronized void cancelGroupConfigChangeTask() {
        if (currentGroupConfigChangeTask == GroupConfigChangeTask.NONE) {
            return;
        }
        logger.info("cancel group config change task");
        currentGroupConfigChangeTaskReference.cancel();
        currentGroupConfigChangeTask = GroupConfigChangeTask.NONE;
        currentGroupConfigChangeTaskReference = new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.OK);
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

        int newTerm = role.getTerm() + 1;
        role.cancelTimeoutOrTask();

        if (context.group().isStandalone()) {
            if (context.mode() == NodeMode.STANDBY) {
                logger.info("starts with standby mode, skip election");
            } else {

                // become leader
                logger.info("become leader, term {}", newTerm);
                resetReplicatingStates();
                changeToRole(new LeaderNodeRole(newTerm, scheduleLogReplicationTask()));
                context.log().appendEntry(newTerm); // no-op log
            }
        } else {
            if (role.getName() == RoleName.FOLLOWER) {
                // pre-vote
                changeToRole(new FollowerNodeRole(role.getTerm(), null, null, 1, 0, scheduleElectionTimeout()));

                EntryMeta lastEntryMeta = context.log().getLastEntryMeta();
                PreVoteRpc rpc = new PreVoteRpc();
                rpc.setTerm(role.getTerm());
                rpc.setLastLogIndex(lastEntryMeta.getIndex());
                rpc.setLastLogTerm(lastEntryMeta.getTerm());
                context.connector().sendPreVote(rpc, context.group().listEndpointOfMajorExceptSelf());
            } else {
                // candidate
                startElection(newTerm);
            }
        }
    }

    private void startElection(int term) {
        logger.info("start election, term {}", term);
        changeToRole(new CandidateNodeRole(term, scheduleElectionTimeout()));

        // request vote
        EntryMeta lastEntryMeta = context.log().getLastEntryMeta();
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(term);
        rpc.setCandidateId(context.selfId());
        rpc.setLastLogIndex(lastEntryMeta.getIndex());
        rpc.setLastLogTerm(lastEntryMeta.getTerm());
        context.connector().sendRequestVote(rpc, context.group().listEndpointOfMajorExceptSelf());
    }

    /**
     * Become follower.
     *
     * @param term                    term
     * @param votedFor                voted for
     * @param leaderId                leader id
     * @param lastHeartbeat           last heart beat
     * @param scheduleElectionTimeout schedule election timeout or not
     */
    private void becomeFollower(int term, NodeId votedFor, NodeId leaderId, long lastHeartbeat, boolean scheduleElectionTimeout) {
        role.cancelTimeoutOrTask();
        if (leaderId != null && !leaderId.equals(role.getLeaderId(context.selfId()))) {
            logger.info("current leader is {}, term {}", leaderId, term);
        }
        ElectionTimeout electionTimeout = scheduleElectionTimeout ? scheduleElectionTimeout() : ElectionTimeout.NONE;
        changeToRole(new FollowerNodeRole(term, votedFor, leaderId, 0, lastHeartbeat, electionTimeout));
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
    private boolean isStableBetween(AbstractNodeRole before, AbstractNodeRole after) {
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
        context.group().resetReplicatingStates(context.log().getNextIndex());
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
        if (context.group().isStandalone()) {
            context.log().advanceCommitIndex(context.log().getNextIndex() - 1, role.getTerm());
            return;
        }
        logger.debug("replicate log");
        for (GroupMember member : context.group().listReplicationTarget()) {
            if (member.shouldReplicate(context.config().getLogReplicationReadTimeout())) {
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
        member.replicateNow();
        try {
            AppendEntriesRpc rpc = context.log().createAppendEntriesRpc(role.getTerm(), context.selfId(), member.getNextIndex(), maxEntries);
            context.connector().sendAppendEntries(rpc, member.getEndpoint());
        } catch (EntryInSnapshotException ignored) {
            logger.debug("log entry {} in snapshot, replicate with install snapshot RPC", member.getNextIndex());
            InstallSnapshotRpc rpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(), 0, context.config().getSnapshotDataLength());
            context.connector().sendInstallSnapshot(rpc, member.getEndpoint());
        }
    }

    @Subscribe
    public void onReceivePreVoteRpc(PreVoteRpcMessage rpcMessage) {
        context.taskExecutor().submit(() ->
                        context.connector().replyPreVote(doProcessPreVoteRpc(rpcMessage), rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private PreVoteResult doProcessPreVoteRpc(PreVoteRpcMessage rpcMessage) {
        PreVoteRpc rpc = rpcMessage.get();
        return new PreVoteResult(role.getTerm(), !context.log().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm()));
    }

    @Subscribe
    public void onReceivePreVoteResult(PreVoteResult result) {
        context.taskExecutor().submit(() -> doProcessPreVoteResult(result), LOGGING_FUTURE_CALLBACK);
    }

    private void doProcessPreVoteResult(PreVoteResult result) {
        if (role.getName() != RoleName.FOLLOWER) {
            logger.warn("receive pre vote result when current role is not follower, ignore");
            return;
        }
        if (!result.isVoteGranted()) {
            return;
        }
        int currentPreVotesCount = ((FollowerNodeRole) role).getPreVotesCount() + 1;
        int countOfMajor = context.group().getCountOfMajor();
        logger.debug("pre votes count {}, major node count {}", currentPreVotesCount, countOfMajor);
        role.cancelTimeoutOrTask();
        if (currentPreVotesCount > countOfMajor / 2) {
            startElection(role.getTerm() + 1);
        } else {
            changeToRole(new FollowerNodeRole(role.getTerm(), null, null, currentPreVotesCount, 0, scheduleElectionTimeout()));
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
                () -> {
                    RequestVoteResult result = doProcessRequestVoteRpc(rpcMessage);
                    if (result != null) {
                        context.connector().replyRequestVote(result, rpcMessage);
                    }
                },
                LOGGING_FUTURE_CALLBACK
        );
    }

    private RequestVoteResult doProcessRequestVoteRpc(RequestVoteRpcMessage rpcMessage) {

        // skip non-major node, it maybe removed node
//        if (!context.group().isMemberOfMajor(rpcMessage.getSourceNodeId())) {
//            logger.warn("receive request vote rpc from node {} which is not major node, ignore", rpcMessage.getSourceNodeId());
//            return new RequestVoteResult(role.getTerm(), false);
//        }

        if (role.getName() == RoleName.FOLLOWER &&
                (System.currentTimeMillis() - ((FollowerNodeRole) role).getLastHeartbeat() < context.config().getMinElectionTimeout())) {
            return null;
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
            becomeFollower(rpc.getTerm(), (voteForCandidate ? rpc.getCandidateId() : null), null, 0, true);
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
                    becomeFollower(role.getTerm(), rpc.getCandidateId(), null, 0, true);
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
            becomeFollower(result.getTerm(), null, null, 0, true);
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
            logger.info("become leader, term {}", role.getTerm());
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
            becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), System.currentTimeMillis(), true);
            return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), appendEntries(rpc));
        }

        assert rpc.getTerm() == role.getTerm();
        switch (role.getName()) {
            case FOLLOWER:

                // reset election timeout and append entries
                becomeFollower(rpc.getTerm(), ((FollowerNodeRole) role).getVotedFor(), rpc.getLeaderId(), System.currentTimeMillis(), true);
                return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), appendEntries(rpc));
            case CANDIDATE:

                // more than one candidate but another node won the election
                becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), System.currentTimeMillis(), true);
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
        AppendEntriesState state = context.log().appendEntriesFromLeader(rpc.getPrevLogIndex(), rpc.getPrevLogTerm(), rpc.getEntries());
        if (state.isSuccess()) {
            if (state.hasGroup()) {
                context.group().updateNodes(state.getLatestGroup());
            }
            context.log().advanceCommitIndex(Math.min(rpc.getLeaderCommit(), rpc.getLastEntryIndex()), rpc.getTerm());
            return true;
        }
        return false;
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
            becomeFollower(result.getTerm(), null, null, 0, true);
            return;
        }

        // check role
        if (role.getName() != RoleName.LEADER) {
            logger.warn("receive append entries result from node {} but current node is not leader, ignore", resultMessage.getSourceNodeId());
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
            // peer
            // advance commit index if major of match index changed
            if (member.advanceReplicatingState(rpc.getLastEntryIndex())) {
                List<GroupConfigEntry> groupConfigEntries = context.log().advanceCommitIndex(context.group().getMatchIndexOfMajor(), role.getTerm());
                for (GroupConfigEntry entry : groupConfigEntries) {
                    currentGroupConfigChangeTask.onLogCommitted(entry);
                }
            }

            for (ReadIndexTask task : readIndexTaskMap.values()) {
                if (task.updateMatchIndex(member.getId(), member.getMatchIndex())) {
                    logger.debug("remove read index task {}", task);
                    readIndexTaskMap.remove(task.getRequestId());
                }
            }

            // node caught up
            if (member.getNextIndex() >= context.log().getNextIndex()) {
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
            becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), System.currentTimeMillis(), true);
        }
        InstallSnapshotState state = context.log().installSnapshot(rpc);
        if (state.getStateName() == InstallSnapshotState.StateName.INSTALLED) {
            context.group().updateNodes(state.getLastConfig());
        }
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
            becomeFollower(result.getTerm(), null, null, 0, true);
            return;
        }

        // check role
        if (role.getName() != RoleName.LEADER) {
            logger.warn("receive install snapshot result from node {} but current node is not leader, ignore", resultMessage.getSourceNodeId());
            return;
        }

        // dispatch to new node catch up task by node id
        if (newNodeCatchUpTaskGroup.onReceiveInstallSnapshotResult(resultMessage, context.log().getNextIndex())) {
            return;
        }

        NodeId sourceNodeId = resultMessage.getSourceNodeId();
        GroupMember member = context.group().getMember(sourceNodeId);
        if (member == null) {
            logger.info("unexpected install snapshot result from node {}, node maybe removed", sourceNodeId);
            return;
        }

        InstallSnapshotRpc rpc = resultMessage.getRpc();
        if (rpc.isDone()) {

            // change to append entries rpc
            member.advanceReplicatingState(rpc.getLastIndex());
            int maxEntries = member.isMajor() ? context.config().getMaxReplicationEntries() : context.config().getMaxReplicationEntriesForNewNode();
            doReplicateLog(member, maxEntries);
        } else {

            // transfer data
            InstallSnapshotRpc nextRpc = context.log().createInstallSnapshotRpc(
                    role.getTerm(), rpc.getLastIndex(), context.selfId(),
                    rpc.getOffset() + rpc.getDataLength(), context.config().getSnapshotDataLength());
            context.connector().sendInstallSnapshot(nextRpc, member.getEndpoint());
        }
    }

    /**
     * Generate snapshot.
     * <p>
     * Source: log.
     * </p>
     *
     * @param event event
     */
    @Subscribe
    public void onGenerateSnapshot(SnapshotGeneratedEvent event) {
        context.taskExecutor().submit(() -> {
            context.log().snapshotGenerated(event.getLastIncludedIndex());
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
                AppendEntriesRpc rpc = context.log().createAppendEntriesRpc(role.getTerm(), context.selfId(), nextIndex, context.config().getMaxReplicationEntriesForNewNode());
                context.connector().sendAppendEntries(rpc, endpoint);
            } catch (EntryInSnapshotException ignored) {

                // change to install snapshot rpc if entry in snapshot
                logger.debug("log entry {} in snapshot, replicate with install snapshot RPC", nextIndex);
                InstallSnapshotRpc rpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(), 0, context.config().getSnapshotDataLength());
                context.connector().sendInstallSnapshot(rpc, endpoint);
            }
        }

        @Override
        public void sendInstallSnapshot(NodeEndpoint endpoint, int offset) {
            InstallSnapshotRpc rpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(), offset, context.config().getSnapshotDataLength());
            context.connector().sendInstallSnapshot(rpc, endpoint);
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
                Set<NodeEndpoint> nodeEndpoints = context.group().listEndpointOfMajor();
                AddNodeEntry entry = context.log().appendEntryForAddNode(role.getTerm(), nodeEndpoints, endpoint);
                assert !context.selfId().equals(endpoint.getId());
                context.group().addNode(endpoint, nextIndex, matchIndex, true);
                currentGroupConfigChangeTask.setGroupConfigEntry(entry);
                NodeImpl.this.doReplicateLog();
            }, LOGGING_FUTURE_CALLBACK);
        }

        @Override
        public void downgradeSelf() {
            becomeFollower(role.getTerm(), null, null, 0, false);
        }

        @Override
        public void removeNode(NodeId nodeId) {
            context.taskExecutor().submit(() -> {
                Set<NodeEndpoint> nodeEndpoints = context.group().listEndpointOfMajor();
                RemoveNodeEntry entry = context.log().appendEntryForRemoveNode(role.getTerm(), nodeEndpoints, nodeId);
                context.group().removeNode(nodeId);
                currentGroupConfigChangeTask.setGroupConfigEntry(entry);
                NodeImpl.this.doReplicateLog();
            }, LOGGING_FUTURE_CALLBACK);
        }


        @Override
        public void done() {
            synchronized (NodeImpl.this) {
                currentGroupConfigChangeTask = GroupConfigChangeTask.NONE;
                currentGroupConfigChangeTaskReference = new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.OK);
            }
        }

    }

}
