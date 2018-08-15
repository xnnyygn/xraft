package in.xnnyygn.xraft.core.service;

import in.xnnyygn.xraft.core.node.NodeId;

public class RemoveNodeCommand {

    private final NodeId nodeId;

    public RemoveNodeCommand(String nodeId) {
        this.nodeId = new NodeId(nodeId);
    }

    public NodeId getNodeId() {
        return nodeId;
    }

}
