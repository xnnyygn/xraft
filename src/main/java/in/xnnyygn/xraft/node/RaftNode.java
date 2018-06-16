package in.xnnyygn.xraft.node;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import in.xnnyygn.xraft.messages.SimpleMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftNode extends AbstractRaftNode {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);
    private final ActorSystem actorSystem;

    public RaftNode(RaftNodeId id, ActorSystem actorSystem) {
        super(id);
        this.actorSystem = actorSystem;
    }

    public void start() {
        logger.info("start raft node {}", getId());
        this.actorSystem.actorSelection("/user/nodestate").tell(new SimpleMessage(SimpleMessage.Kind.START_UP), ActorRef.noSender());
    }

    public void stop() {
        logger.info("stop raft node {}", getId());
        this.actorSystem.terminate();
    }

    public ActorSelection getRpcEndpoint() {
        return this.actorSystem.actorSelection("/user/rpc");
    }

}
