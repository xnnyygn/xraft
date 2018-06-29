package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.service.ServerRouter;
import in.xnnyygn.xraft.kvstore.message.GetCommand;
import in.xnnyygn.xraft.kvstore.message.SetCommand;

public class Client {

    private final ServerRouter serverRouter;

    public Client(ServerRouter serverRouter) {
        this.serverRouter = serverRouter;
    }

    public void set(String key, byte[] value) {
        this.serverRouter.send(new SetCommand(key, value));
    }

    public byte[] get(String key) {
        return (byte[]) this.serverRouter.send(new GetCommand(key));
    }

}
