package in.xnnyygn.xraft.core.server;

import in.xnnyygn.xraft.core.serverstate.ServerStateMachine;
import in.xnnyygn.xraft.core.rpc.Channel;
import in.xnnyygn.xraft.core.serverstate.ServerStateSnapshot;
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

    public ServerStateSnapshot getServerState() {
        return this.serverStateMachine.takeSnapshot();
    }

    // TODO internal channel, change to package visible
    public Channel getRpcChannel() {
        return this.rpcChannel;
    }

    public void stop() throws Exception {
        logger.info("stop server {}", getId());
        this.serverStateMachine.stop();
        this.rpcChannel.close();
    }

}
