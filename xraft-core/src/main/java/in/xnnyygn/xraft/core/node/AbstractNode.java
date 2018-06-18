package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.rpc.Channel;

public abstract class AbstractNode {

    private final NodeId id;

    AbstractNode(NodeId id) {
        this.id = id;
    }

    public NodeId getId() {
        return id;
    }

    public abstract Channel getRpcChannel();

}
