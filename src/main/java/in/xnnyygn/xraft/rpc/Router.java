package in.xnnyygn.xraft.rpc;

import in.xnnyygn.xraft.server.AbstractServer;
import in.xnnyygn.xraft.server.Server;
import in.xnnyygn.xraft.server.ServerGroup;
import in.xnnyygn.xraft.server.ServerId;

public class Router {

    private final ServerGroup serverGroup;
    private final ServerId selfServerId;

    public Router(ServerGroup serverGroup, ServerId selfServerId) {
        this.serverGroup = serverGroup;
        this.selfServerId = selfServerId;
    }

    public void sendRpc(Object rpc) {
        for (AbstractServer server : serverGroup) {
            if (server.getId() != this.selfServerId && (server instanceof Server)) {
                ((Server) server).getRpcChannel().write(rpc, this.selfServerId);
            }
        }
    }

    public void sendResult(Object result, ServerId destination) {
        AbstractServer server = this.serverGroup.findServer(destination);
        if (server instanceof Server) {
            ((Server) server).getRpcChannel().write(result, this.selfServerId);
        }
    }

}
