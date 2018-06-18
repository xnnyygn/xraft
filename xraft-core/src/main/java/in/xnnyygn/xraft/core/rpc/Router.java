package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.server.AbstractServer;
import in.xnnyygn.xraft.core.server.ServerGroup;
import in.xnnyygn.xraft.core.server.ServerId;

public class Router {

    private final ServerGroup serverGroup;
    private final ServerId selfServerId;

    public Router(ServerGroup serverGroup, ServerId selfServerId) {
        this.serverGroup = serverGroup;
        this.selfServerId = selfServerId;
    }

    public void sendRpc(Object rpc) {
        for (AbstractServer server : serverGroup) {
            if (server.getId() != this.selfServerId) {
                server.getRpcChannel().write(rpc, this.selfServerId);
            }
        }
    }

    public void sendResult(Object result, ServerId destination) {
        AbstractServer server = this.serverGroup.find(destination);
        server.getRpcChannel().write(result, this.selfServerId);
    }

}
