package in.xnnyygn.xraft.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;
import in.xnnyygn.xraft.server.AbstractServer;
import in.xnnyygn.xraft.server.Server;
import in.xnnyygn.xraft.server.ServerGroup;
import in.xnnyygn.xraft.server.ServerId;
import in.xnnyygn.xraft.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(RpcActor.class);
    private final ServerGroup nodeGroup;
    private final ServerId selfNodeId;

    public RpcActor(ServerGroup nodeGroup, ServerId selfNodeId) {
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
            AbstractServer node = this.nodeGroup.findNode(msg.getDestinationServerId());
            msg.setDestinationServerId(null);
            msg.setSenderServerId(this.selfNodeId);

            sendMessageToNode(node, msg);
        } else {
            forwardToElectionActor(msg);
        }
    }

    private void forwardToElectionActor(RaftMessage msg) {
        getElectionActor().tell(msg, getSelf());
    }

    private void sendMessageToPeers(RaftMessage msg) {
        for (AbstractServer node : nodeGroup) {
            this.sendMessageToNode(node, msg);
        }
    }

    private void sendMessageToNode(AbstractServer node, RaftMessage msg) {
        if (!node.getId().equals(this.selfNodeId) && (node instanceof Server)) {
            logger.debug("Node {}, send {} to peer {}", this.selfNodeId, msg, node.getId());
            ((Server) node).getRpcEndpoint().tell(msg, getSelf());
        }
    }

}
