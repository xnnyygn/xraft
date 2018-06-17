package in.xnnyygn.xraft.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;
import in.xnnyygn.xraft.serverstate.*;
import in.xnnyygn.xraft.schedule.ElectionTimeout;
import in.xnnyygn.xraft.schedule.LogReplicationTask;
import in.xnnyygn.xraft.schedule.Scheduler;
import in.xnnyygn.xraft.messages.*;
import in.xnnyygn.xraft.server.ServerGroup;
import in.xnnyygn.xraft.server.ServerId;
import in.xnnyygn.xraft.server.ServerStore;
import in.xnnyygn.xraft.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.rpc.RequestVoteResult;
import in.xnnyygn.xraft.rpc.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElectionActor extends AbstractActor implements ServerStateContext {

    private static final Logger logger = LoggerFactory.getLogger(ElectionActor.class);

    private AbstractServerState serverState;

    private final ServerGroup serverGroup;
    private final ServerId selfServerId;
    private final ServerStore serverStore;

    private final Scheduler scheduler;

    public ElectionActor(ServerGroup serverGroup, ServerId selfServerId, ServerStore serverStore) {
        super();
        this.serverGroup = serverGroup;
        this.selfServerId = selfServerId;
        this.serverStore = serverStore;

        this.scheduler = new Scheduler(selfServerId, getContext().getSystem());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(SimpleMessage.class, msg -> {
            switch (msg.getKind()) {
                case START_UP:
                    startUp();
                    break;
                case ELECTION_TIMEOUT:
                    onElectionTimeout();
                    break;
            }
        }).match(RequestVoteRpcMessage.class, msg -> {
            onReceiveRequestVoteRpc(msg.getRpc());
        }).match(RequestVoteResultMessage.class, msg -> {
            onReceiveRequestVoteResult(msg.getResult(), msg.getSenderServerId());
        }).match(AppendEntriesRpcMessage.class, msg -> {
            onReceiveAppendEntriesRpc(msg.getRpc());
        }).build();
    }

    @Override
    public void postStop() throws Exception {
        logger.debug("Server {}, stop scheduler", this.selfServerId);
        this.scheduler.stop();
    }

    private ActorSelection getRpcActor() {
        return getContext().actorSelection(RaftActorPaths.ACTOR_PATH_RPC);
    }

    private void startUp() {
        ElectionTimeout electionTimeout = this.scheduler.scheduleElectionTimeout();
        this.serverState = new FollowerServerState(this.serverStore, electionTimeout);
        logger.debug("Server {}, start with state {}", this.selfServerId, this.serverState);
        serverStateChanged(this.serverState.takeSnapshot());
    }

    private void onElectionTimeout() {
        logger.debug("Server {}, election timeout", this.selfServerId);
        this.serverState.onElectionTimeout(this);
    }

    private void replicateLog() {
        logger.debug("Server {}, replicate log", this.selfServerId);
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(this.serverState.getTerm());
        rpc.setLeaderId(this.selfServerId);
        getRpcActor().tell(new AppendEntriesRpcMessage(rpc), getSelf());
    }

    private void onReceiveRequestVoteResult(RequestVoteResult result, ServerId senderServerId) {
        logger.debug("Server {}, receive {} from peer {}", this.selfServerId, result, senderServerId);
        this.serverState.onReceiveRequestVoteResult(this, result);
    }

    private void onReceiveRequestVoteRpc(RequestVoteRpc rpc) {
        logger.debug("Server {}, receive {} from peer {}", this.selfServerId, rpc, rpc.getCandidateId());
        this.serverState.onReceiveRequestVoteRpc(this, rpc);
    }

    private void onReceiveAppendEntriesRpc(AppendEntriesRpc rpc) {
        logger.debug("Server {}, receive {} from leader {}", this.selfServerId, rpc, rpc.getLeaderId());
        this.serverState.onReceiveAppendEntriesRpc(this, rpc);
    }

    @Override
    public ServerId getSelfServerId() {
        return this.selfServerId;
    }

    @Override
    public int getServerCount() {
        return this.serverGroup.getServerCount();
    }

    @Override
    public void setServerState(AbstractServerState serverState) {
        logger.debug("Server {}, state changed {} -> {}", this.selfServerId, this.serverState, serverState);
        this.serverState = serverState;
        serverStateChanged(this.serverState.takeSnapshot());
    }

    @Override
    public LogReplicationTask scheduleLogReplicationTask() {
        return this.scheduler.scheduleLogReplicationTask(this::replicateLog);
    }

    @Override
    public void sendRpcOrResultMessage(RaftMessage message) {
        getRpcActor().tell(message, getSelf());
    }

    @Override
    public ElectionTimeout scheduleElectionTimeout() {
        return this.scheduler.scheduleElectionTimeout();
    }

    ///////////////

    private ServerStateSnapshot lastServerState;

    private void serverStateChanged(ServerStateSnapshot snapshot) {
        if (lastServerState == null || !isStable(lastServerState, snapshot)) {
            logger.info("Server {}, state changed -> {}", this.selfServerId, snapshot);
            lastServerState = snapshot;
        }
    }

    private boolean isStable(ServerStateSnapshot stateBefore, ServerStateSnapshot stateAfter) {
        return stateBefore.getRole() == ServerRole.FOLLOWER &&
                stateAfter.getRole() == stateBefore.getRole() &&
                stateAfter.getTerm() == stateBefore.getTerm() &&
                stateAfter.getLeaderId() == stateBefore.getLeaderId() &&
                stateAfter.getVotedFor() == stateBefore.getVotedFor();
    }

}
