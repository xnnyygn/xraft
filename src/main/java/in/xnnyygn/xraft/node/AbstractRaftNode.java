package in.xnnyygn.xraft.node;

public abstract class AbstractRaftNode {

    private final RaftNodeId id;

    AbstractRaftNode(RaftNodeId id) {
        this.id = id;
    }

    public RaftNodeId getId() {
        return id;
    }

}
