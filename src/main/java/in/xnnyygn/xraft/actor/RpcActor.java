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
    private final ServerGroup serverGroup;
    private final ServerId selfServerId;

    public RpcActor(ServerGroup serverGroup, ServerId selfServerId) {
        super();
        this.serverGroup = serverGroup;
        this.selfServerId = selfServerId;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(RequestVoteRpcMessage.class, msg -> {
            if (msg.getSenderServerId().equals(this.selfServerId)) {
                sendMessageToPeers(msg);
            } else {
                forwardToElectionActor(msg);
            }
        }).match(RequestVoteResultMessage.class, this::processResultMessage
        ).match(AppendEntriesRpcMessage.class, msg -> {
            if (msg.getSenderServerId().equals(this.selfServerId)) {
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
        if (msg.isDestinationServerIdPresent()) {
            AbstractServer server = this.serverGroup.findServer(msg.getDestinationServerId());
            msg.setDestinationServerId(null);
            msg.setSenderServerId(this.selfServerId);

            sendMessageToServer(server, msg);
        } else {
            forwardToElectionActor(msg);
        }
    }

    private void forwardToElectionActor(RaftMessage msg) {
        getElectionActor().tell(msg, getSelf());
    }

    private void sendMessageToPeers(RaftMessage msg) {
        for (AbstractServer server : serverGroup) {
            this.sendMessageToServer(server, msg);
        }
    }

    private void sendMessageToServer(AbstractServer server, RaftMessage msg) {
        if (!server.getId().equals(this.selfServerId) && (server instanceof Server)) {
            logger.debug("Server {}, send {} to peer {}", this.selfServerId, msg, server.getId());
            ((Server) server).getRpcEndpoint().tell(msg, getSelf());
        }
    }

}
