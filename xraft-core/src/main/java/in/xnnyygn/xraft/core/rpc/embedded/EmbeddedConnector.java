package in.xnnyygn.xraft.core.rpc.embedded;

import in.xnnyygn.xraft.core.node.AbstractNode;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.AbstractConnector;
import in.xnnyygn.xraft.core.rpc.message.AbstractRpcMessage;
import in.xnnyygn.xraft.core.rpc.Channel;
import in.xnnyygn.xraft.core.rpc.Endpoint;

public class EmbeddedConnector extends AbstractConnector {

    public EmbeddedConnector(NodeGroup nodeGroup, NodeId selfNodeId) {
        super(nodeGroup, selfNodeId);
    }

    @Override
    protected Channel getChannel(AbstractNode node) {
        Endpoint endpoint = node.getEndpoint();
        if (endpoint instanceof EmbeddedEndpoint) {
            return ((EmbeddedEndpoint) endpoint).getChannel();
        }
        throw new IllegalArgumentException("expected embedded endpoint");
    }

    @Override
    protected <T> Channel getChannel(AbstractRpcMessage<T> rpcMessage) {
        return getChannel(this.nodeGroup.find(rpcMessage.getSourceNodeId()));
    }

}
