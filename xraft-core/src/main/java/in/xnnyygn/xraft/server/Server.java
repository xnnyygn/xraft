package in.xnnyygn.xraft.server;

import in.xnnyygn.xraft.rpc.Channel;
import in.xnnyygn.xraft.serverstate.ServerStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server extends AbstractServer {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final ServerStateMachine serverStateMachine;
    private final Channel rpcChannel;

    public Server(ServerId id, ServerStateMachine serverStateMachine, Channel rpcChannel) {
        super(id);
        this.serverStateMachine = serverStateMachine;
        this.rpcChannel = rpcChannel;
    }

    public void start() {
        logger.info("start server {}", getId());
        this.serverStateMachine.start();
    }

    public Channel getRpcChannel() {
        return this.rpcChannel;
    }

    public void stop() throws Exception {
        logger.info("stop server {}", getId());
        this.serverStateMachine.stop();
        this.rpcChannel.close();
    }

}
