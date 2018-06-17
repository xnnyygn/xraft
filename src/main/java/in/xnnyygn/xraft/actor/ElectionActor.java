package in.xnnyygn.xraft.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;
import in.xnnyygn.xraft.serverstate.*;
import in.xnnyygn.xraft.scheduler.ElectionTimeout;
import in.xnnyygn.xraft.scheduler.LogReplicationTask;
import in.xnnyygn.xraft.scheduler.RaftScheduler;
import in.xnnyygn.xraft.messages.*;
import in.xnnyygn.xraft.node.RaftNodeGroup;
import in.xnnyygn.xraft.node.RaftNodeId;
import in.xnnyygn.xraft.node.RaftNodeSave;
import in.xnnyygn.xraft.rpc.AppendEntriesRpc;
import in.xnnyygn.xraft.rpc.RequestVoteResult;
import in.xnnyygn.xraft.rpc.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElectionActor extends AbstractActor implements NodeStateContext {

    private static final Logger logger = LoggerFactory.getLogger(ElectionActor.class);

    private AbstractNodeState nodeState;

    private final RaftNodeGroup nodeGroup;
    private final RaftNodeId selfNodeId;
    private final RaftNodeSave nodeSave;

    private final RaftScheduler scheduler;

    public ElectionActor(RaftNodeGroup nodeGroup, RaftNodeId selfNodeId, RaftNodeSave nodeSave) {
        super();
        this.nodeGroup = nodeGroup;
        this.selfNodeId = selfNodeId;
        this.nodeSave = nodeSave;

        this.scheduler = new RaftScheduler(selfNodeId, getContext().getSystem());
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
                case LOG_REPLICATION:
                    replicateLog();
                    break;
            }
        }).match(RequestVoteRpcMessage.class, msg -> {
            onReceiveRequestVoteRpc(msg.getRpc());
        }).match(RequestVoteResultMessage.class, msg -> {
            onReceiveRequestVoteResult(msg.getResult(), msg.getSenderNodeId());
        }).match(AppendEntriesRpcMessage.class, msg -> {
            onReceiveAppendEntriesRpc(msg.getRpc());
        }).build();
    }

    @Override
    public void postStop() throws Exception {
        logger.debug("Node {}, stop scheduler", this.selfNodeId);
        this.scheduler.terminate();
    }

    private ActorSelection getRpcActor() {
        return getContext().actorSelection(RaftActorPaths.ACTOR_PATH_RPC);
    }

    private void startUp() {
        ElectionTimeout electionTimeout = this.scheduler.scheduleElectionTimeout();
        this.nodeState = new FollowerNodeState(this.nodeSave, electionTimeout);
        logger.debug("Node {}, start with state {}", this.selfNodeId, this.nodeState);
        nodeStateChanged(this.nodeState.takeSnapshot());
    }

    private void onElectionTimeout() {
        logger.debug("Node {}, election timeout", this.selfNodeId);
        this.nodeState.onElectionTimeout(this);
    }

    private void replicateLog() {
        logger.debug("Node {}, replicate log", this.selfNodeId);
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(this.nodeState.getTerm());
        rpc.setLeaderId(this.selfNodeId);
        getRpcActor().tell(new AppendEntriesRpcMessage(rpc), getSelf());
    }

    private void onReceiveRequestVoteResult(RequestVoteResult result, RaftNodeId senderNodeId) {
        logger.debug("Node {}, receive {} from peer {}", this.selfNodeId, result, senderNodeId);
        this.nodeState.onReceiveRequestVoteResult(this, result);
    }

    private void onReceiveRequestVoteRpc(RequestVoteRpc rpc) {
        logger.debug("Node {}, receive {} from peer {}", this.selfNodeId, rpc, rpc.getCandidateId());
        this.nodeState.onReceiveRequestVoteRpc(this, rpc);
    }

    private void onReceiveAppendEntriesRpc(AppendEntriesRpc rpc) {
        logger.debug("Node {}, receive {} from leader {}", this.selfNodeId, rpc, rpc.getLeaderId());
        this.nodeState.onReceiveAppendEntriesRpc(this, rpc);
    }

    @Override
    public RaftNodeId getSelfNodeId() {
        return this.selfNodeId;
    }

    @Override
    public int getNodeCount() {
        return this.nodeGroup.getNodeCount();
    }

    @Override
    public void setNodeState(AbstractNodeState nodeState) {
        logger.debug("Node {}, state changed {} -> {}", this.selfNodeId, this.nodeState, nodeState);
        this.nodeState = nodeState;
        nodeStateChanged(this.nodeState.takeSnapshot());
    }

    @Override
    public LogReplicationTask scheduleLogReplicationTask() {
        return this.scheduler.scheduleLogReplicationTask();
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

    private NodeStateSnapshot lastNodeState;

    private void nodeStateChanged(NodeStateSnapshot snapshot) {
        if (lastNodeState == null || !isStable(lastNodeState, snapshot)) {
            logger.info("Node {}, state changed -> {}", this.selfNodeId, snapshot);
            lastNodeState = snapshot;
        }
    }

    private boolean isStable(NodeStateSnapshot stateBefore, NodeStateSnapshot stateAfter) {
        return stateBefore.getRole() == NodeRole.FOLLOWER &&
                stateAfter.getRole() == stateBefore.getRole() &&
                stateAfter.getTerm() == stateBefore.getTerm() &&
                stateAfter.getLeaderId() == stateBefore.getLeaderId() &&
                stateAfter.getVotedFor() == stateBefore.getVotedFor();
    }

}
