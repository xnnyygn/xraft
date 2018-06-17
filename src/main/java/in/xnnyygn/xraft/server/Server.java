package in.xnnyygn.xraft.server;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import in.xnnyygn.xraft.messages.SimpleMessage;
import in.xnnyygn.xraft.rpc.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server extends AbstractServer {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final ActorSystem actorSystem;
    private final Channel rpcChannel;

    public Server(ServerId id, ActorSystem actorSystem, Channel rpcChannel) {
        super(id);
        this.actorSystem = actorSystem;
        this.rpcChannel = rpcChannel;
    }

    public void start() {
        logger.info("start server {}", getId());
        this.actorSystem.actorSelection("/user/election").tell(new SimpleMessage(SimpleMessage.Kind.START_UP), ActorRef.noSender());
    }

    public void stop() {
        logger.info("stop server {}", getId());
        this.actorSystem.terminate();
    }

    @Deprecated
    public ActorSelection getRpcEndpoint() {
        return this.actorSystem.actorSelection("/user/rpc");
    }

    public Channel getRpcChannel() {
        return this.rpcChannel;
    }

}
