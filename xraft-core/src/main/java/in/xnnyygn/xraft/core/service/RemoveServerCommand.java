package in.xnnyygn.xraft.core.service;

import in.xnnyygn.xraft.core.node.NodeId;

public class RemoveServerCommand {

    private final NodeId nodeId;

    public RemoveServerCommand(String nodeId) {
        this.nodeId = new NodeId(nodeId);
    }

    public NodeId getNodeId() {
        return nodeId;
    }

}
