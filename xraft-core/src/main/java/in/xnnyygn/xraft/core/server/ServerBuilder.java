package in.xnnyygn.xraft.core.server;

import in.xnnyygn.xraft.core.nodestate.LoggingNodeStateListener;
import in.xnnyygn.xraft.core.nodestate.NodeStateMachine;
import in.xnnyygn.xraft.core.rpc.EmbeddedChannel;
import in.xnnyygn.xraft.core.rpc.Router;

public class ServerBuilder {

    private final String serverId;
    private final ServerGroup serverGroup;
    private ServerStore serverStore = new ServerStore();

    public ServerBuilder(String serverId, ServerGroup serverGroup) {
        this.serverId = serverId;
        this.serverGroup = serverGroup;
    }

    public Server build() {
        ServerId selfServerId = new ServerId(this.serverId);
        Router rpcRouter = new Router(this.serverGroup, selfServerId);
        NodeStateMachine serverStateMachine = new NodeStateMachine(this.serverGroup, selfServerId, this.serverStore, rpcRouter);
        serverStateMachine.addServerStateListener(new LoggingNodeStateListener(selfServerId));
        Server server = new Server(selfServerId, serverStateMachine, new EmbeddedChannel(selfServerId, serverStateMachine));
        serverGroup.add(server);
        return server;
    }

}
