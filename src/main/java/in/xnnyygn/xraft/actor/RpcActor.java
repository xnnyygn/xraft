package in.xnnyygn.xraft.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;
import in.xnnyygn.xraft.server.AbstractRaftNode;
import in.xnnyygn.xraft.server.RaftNode;
import in.xnnyygn.xraft.server.RaftNodeGroup;
import in.xnnyygn.xraft.server.RaftNodeId;
import in.xnnyygn.xraft.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(RpcActor.class);
    private final RaftNodeGroup nodeGroup;
    private final RaftNodeId selfNodeId;

    public RpcActor(RaftNodeGroup nodeGroup, RaftNodeId selfNodeId) {
        super();
        this.nodeGroup = nodeGroup;
        this.selfNodeId = selfNodeId;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(RequestVoteRpcMessage.class, msg -> {
            if (msg.getSenderNodeId().equals(this.selfNodeId)) {
                sendMessageToPeers(msg);
            } else {
                forwardToElectionActor(msg);
            }
        }).match(RequestVoteResultMessage.class, this::processResultMessage
        ).match(AppendEntriesRpcMessage.class, msg -> {
            if (msg.getSenderNodeId().equals(this.selfNodeId)) {
                sendMessageToPeers(msg);
            } else {
                forwardToElectionActor(msg);
            }
        }).match(AppendEntriesResultMessage.class, this::processResultMessage
        ).build();
    }

    private ActorSelection getElectionActor() {
        return getContext().actorSelection(RaftActorPaths.ACTOR_PATH_ELECTION);
    }

    private <T> void processResultMessage(AbstractResultMessage<T> msg) {
        if (msg.isDestinationNodeIdPresent()) {
            AbstractRaftNode node = this.nodeGroup.findNode(msg.getDestinationNodeId());
            msg.setDestinationNodeId(null);
            msg.setSenderNodeId(this.selfNodeId);

            sendMessageToNode(node, msg);
        } else {
            forwardToElectionActor(msg);
        }
    }

    private void forwardToElectionActor(RaftMessage msg) {
        getElectionActor().tell(msg, getSelf());
    }

    private void sendMessageToPeers(RaftMessage msg) {
        for (AbstractRaftNode node : nodeGroup) {
            this.sendMessageToNode(node, msg);
        }
    }

    private void sendMessageToNode(AbstractRaftNode node, RaftMessage msg) {
        if (!node.getId().equals(this.selfNodeId) && (node instanceof RaftNode)) {
            logger.debug("Node {}, send {} to peer {}", this.selfNodeId, msg, node.getId());
            ((RaftNode) node).getRpcEndpoint().tell(msg, getSelf());
        }
    }

}
