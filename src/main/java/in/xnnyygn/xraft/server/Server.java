package in.xnnyygn.xraft.server;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import in.xnnyygn.xraft.messages.SimpleMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server extends AbstractRaftNode {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final ActorSystem actorSystem;

    public Server(RaftNodeId id, ActorSystem actorSystem) {
        super(id);
        this.actorSystem = actorSystem;
    }

    public void start() {
        logger.info("start server {}", getId());
        this.actorSystem.actorSelection("/user/election").tell(new SimpleMessage(SimpleMessage.Kind.START_UP), ActorRef.noSender());
    }

    public void stop() {
        logger.info("stop server {}", getId());
        this.actorSystem.terminate();
    }

    public ActorSelection getRpcEndpoint() {
        return this.actorSystem.actorSelection("/user/rpc");
    }

}
