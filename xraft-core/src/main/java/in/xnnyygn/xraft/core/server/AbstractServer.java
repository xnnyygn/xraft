package in.xnnyygn.xraft.core.server;

import in.xnnyygn.xraft.core.rpc.Channel;

public abstract class AbstractServer {

    private final ServerId id;

    AbstractServer(ServerId id) {
        this.id = id;
    }

    public ServerId getId() {
        return id;
    }

    public abstract Channel getRpcChannel();

}
