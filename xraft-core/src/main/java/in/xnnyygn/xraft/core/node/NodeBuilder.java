package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.nodestate.LoggingNodeStateListener;
import in.xnnyygn.xraft.core.nodestate.NodeStateMachine;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.rpc.EmbeddedChannel;
import in.xnnyygn.xraft.core.rpc.DefaultConnector;

public class NodeBuilder {

    private final String nodeId;
    private final NodeGroup nodeGroup;
    private NodeStore nodeStore = new NodeStore();

    public NodeBuilder(String nodeId, NodeGroup nodeGroup) {
        this.nodeId = nodeId;
        this.nodeGroup = nodeGroup;
    }

    public Node build() {
        NodeId selfNodeId = new NodeId(this.nodeId);
        Connector rpcConnector = new DefaultConnector(this.nodeGroup, selfNodeId);
        NodeStateMachine nodeStateMachine = new NodeStateMachine(this.nodeGroup, selfNodeId, this.nodeStore, rpcConnector);
        nodeStateMachine.addNodeStateListener(new LoggingNodeStateListener(selfNodeId));
        Node node = new Node(selfNodeId, nodeStateMachine, new EmbeddedChannel(selfNodeId, nodeStateMachine));
        nodeGroup.add(node);
        return node;
    }

}
