package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.node.NodeId;

public class AddNodeTaskReference extends EntryTaskReference {

    private final NodeId nodeId;

    public AddNodeTaskReference(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

}
