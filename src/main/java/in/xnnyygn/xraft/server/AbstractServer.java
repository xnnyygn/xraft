package in.xnnyygn.xraft.server;

public abstract class AbstractServer {

    private final ServerId id;

    AbstractServer(ServerId id) {
        this.id = id;
    }

    public ServerId getId() {
        return id;
    }

}
