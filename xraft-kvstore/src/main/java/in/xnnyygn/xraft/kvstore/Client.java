package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.service.AddServerCommand;
import in.xnnyygn.xraft.core.service.RemoveServerCommand;
import in.xnnyygn.xraft.core.service.ServerRouter;
import in.xnnyygn.xraft.kvstore.message.GetCommand;
import in.xnnyygn.xraft.kvstore.message.SetCommand;

public class Client {

    private final ServerRouter serverRouter;

    public Client(ServerRouter serverRouter) {
        this.serverRouter = serverRouter;
    }

    public void addServer(String nodeId, String host, int port) {
        serverRouter.send(new AddServerCommand(nodeId, host, port));
    }

    public void removeServer(String nodeId) {
        serverRouter.send(new RemoveServerCommand(nodeId));
    }

    public void set(String key, byte[] value) {
        serverRouter.send(new SetCommand(key, value));
    }

    public byte[] get(String key) {
        return (byte[]) serverRouter.send(new GetCommand(key));
    }

}
