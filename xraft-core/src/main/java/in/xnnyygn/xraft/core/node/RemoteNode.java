package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.rpc.Endpoint;

public class RemoteNode extends AbstractNode {

    public RemoteNode(NodeId id, Endpoint endpoint) {
        super(id, endpoint);
    }

}
