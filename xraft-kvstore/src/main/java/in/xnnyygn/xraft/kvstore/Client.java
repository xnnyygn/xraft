package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.service.ServerRouter;
import in.xnnyygn.xraft.kvstore.command.GetCommand;
import in.xnnyygn.xraft.kvstore.command.SetCommand;

public class Client {

    private final ServerRouter serverRouter;

    public Client(ServerRouter serverRouter) {
        this.serverRouter = serverRouter;
    }

    public void set(String key, String value) {
        this.serverRouter.send(new SetCommand(key, value));
    }

    public String get(String key) {
        return (String) this.serverRouter.send(new GetCommand(key));
    }

}
