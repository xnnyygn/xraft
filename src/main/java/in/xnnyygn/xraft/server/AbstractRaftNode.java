package in.xnnyygn.xraft.server;

public abstract class AbstractRaftNode {

    private final ServerId id;

    AbstractRaftNode(ServerId id) {
        this.id = id;
    }

    public ServerId getId() {
        return id;
    }

}
