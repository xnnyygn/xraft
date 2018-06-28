package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.rpc.Endpoint;

public abstract class AbstractNode {

    private final NodeId id;
    private final Endpoint endpoint;

    AbstractNode(NodeId id, Endpoint endpoint) {
        this.id = id;
        this.endpoint = endpoint;
    }

    public NodeId getId() {
        return id;
    }

    public Endpoint getEndpoint() {
        return this.endpoint;
    }

}
