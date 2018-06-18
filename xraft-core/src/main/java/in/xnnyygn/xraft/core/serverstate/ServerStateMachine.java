package in.xnnyygn.xraft.core.serverstate;

import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.RequestVoteRpc;
import in.xnnyygn.xraft.core.rpc.Router;
import in.xnnyygn.xraft.core.schedule.ElectionTimeout;
import in.xnnyygn.xraft.core.schedule.LogReplicationTask;
import in.xnnyygn.xraft.core.schedule.Scheduler;
import in.xnnyygn.xraft.core.server.ServerGroup;
import in.xnnyygn.xraft.core.server.ServerId;
import in.xnnyygn.xraft.core.server.ServerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ServerStateMachine implements ServerStateContext {

    private static final Logger logger = LoggerFactory.getLogger(ServerStateMachine.class);
    private AbstractServerState serverState;

    private final ServerGroup serverGroup;
    private final ServerId selfServerId;
    private final ServerStore serverStore;
    private final Router rpcRouter;

    private final Scheduler scheduler;
    private final List<ServerStateListener> serverStateListeners = new ArrayList<>();

    public ServerStateMachine(ServerGroup serverGroup, ServerId selfServerId, ServerStore serverStore, Router rpcRouter) {
        this.serverGroup = serverGroup;
        this.selfServerId = selfServerId;
        this.serverStore = serverStore;
        this.rpcRouter = rpcRouter;

        this.scheduler = new Scheduler(selfServerId);
    }

    public synchronized void start() {
        this.serverState = new FollowerServerState(this.serverStore, this.scheduleElectionTimeout());
        logger.debug("Server {}, start with state {}", this.selfServerId, this.serverState);
    }

    public synchronized void onReceiveRequestVoteResult(RequestVoteResult result, ServerId senderServerId) {
        logger.debug("Server {}, receive {} from peer {}", this.selfServerId, result, senderServerId);
        this.serverState.onReceiveRequestVoteResult(this, result);
    }

    public synchronized void onReceiveRequestVoteRpc(RequestVoteRpc rpc) {
        logger.debug("Server {}, receive {} from peer {}", this.selfServerId, rpc, rpc.getCandidateId());
        this.serverState.onReceiveRequestVoteRpc(this, rpc);
    }

    public synchronized void onReceiveAppendEntriesRpc(AppendEntriesRpc rpc) {
        logger.debug("Server {}, receive {} from leader {}", this.selfServerId, rpc, rpc.getLeaderId());
        this.serverState.onReceiveAppendEntriesRpc(this, rpc);
    }

    private synchronized void onElectionTimeout() {
        logger.debug("Server {}, election timeout", this.selfServerId);
        this.serverState.onElectionTimeout(this);
    }

    private synchronized void replicateLog() {
        logger.debug("Server {}, replicate log", this.selfServerId);
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(this.serverState.getTerm());
        rpc.setLeaderId(this.selfServerId);
        this.rpcRouter.sendRpc(rpc);
    }

    /**
     * Take snapshot of current server state(without null check).
     * DON'T call before startup or you will get NPE.
     *
     * @return server state snapshot
     */
    public ServerStateSnapshot takeSnapshot() {
        return this.serverState.takeSnapshot();
    }

    @Override
    public ServerId getSelfServerId() {
        return this.selfServerId;
    }

    @Override
    public int getServerCount() {
        return this.serverGroup.getCount();
    }

    @Override
    public void setServerState(AbstractServerState serverState) {
        logger.debug("Server {}, state changed {} -> {}", this.selfServerId, this.serverState, serverState);

        // notify listener if not stable
        if (!isStableBetween(this.serverState, serverState)) {
            ServerStateSnapshot snapshot = serverState.takeSnapshot();
            this.serverStateListeners.forEach((l) -> {
                l.serverStateChanged(snapshot);
            });
        }

        this.serverState = serverState;
    }

    private boolean isStableBetween(AbstractServerState before, AbstractServerState after) {
        return before != null &&
                before.getRole() == ServerRole.FOLLOWER && after.getRole() == ServerRole.FOLLOWER &&
                FollowerServerState.isStableBetween((FollowerServerState) before, (FollowerServerState) after);
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

    public void addServerStateListener(ServerStateListener listener) {
        this.serverStateListeners.add(listener);
    }

}
