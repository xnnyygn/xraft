package in.xnnyygn.xraft.server;

public abstract class AbstractRaftNode {

    private final RaftNodeId id;

    AbstractRaftNode(RaftNodeId id) {
        this.id = id;
    }

    public RaftNodeId getId() {
        return id;
    }

}
