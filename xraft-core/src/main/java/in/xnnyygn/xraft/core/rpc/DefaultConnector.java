package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.AbstractNode;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultConnector implements Connector {

    private static Logger logger = LoggerFactory.getLogger(DefaultConnector.class);
    private final NodeGroup nodeGroup;
    private final NodeId selfNodeId;

    public DefaultConnector(NodeGroup nodeGroup, NodeId selfNodeId) {
        this.nodeGroup = nodeGroup;
        this.selfNodeId = selfNodeId;
    }

    @Override
    public void sendRpc(Object rpc) {
        for (AbstractNode node : nodeGroup) {
            if (node.getId() != this.selfNodeId) {
                logger.debug("send {} to {}", rpc, node.getId());
                node.getChannel().send(rpc, this.selfNodeId);
            }
        }
    }

    @Override
    public void sendRpc(Object rpc, NodeId destinationNodeId) {
        logger.debug("send {} to {}", rpc, destinationNodeId);
        AbstractNode node = this.nodeGroup.find(destinationNodeId);
        node.getChannel().send(rpc, this.selfNodeId);
    }

    @Override
    public void sendResult(Object result, NodeId destinationNodeId) {
        logger.debug("send {} to {}", result, destinationNodeId);
        AbstractNode node = this.nodeGroup.find(destinationNodeId);
        node.getChannel().send(result, this.selfNodeId);
    }

    @Override
    public void sendAppendEntriesResult(AppendEntriesResult result, NodeId destinationNodeId, AppendEntriesRpc rpc) {
        logger.debug("send {} to {}", result, destinationNodeId);
        AbstractNode node = this.nodeGroup.find(destinationNodeId);
        node.getChannel().send(new AppendEntriesResultMessage(result, this.selfNodeId, rpc), this.selfNodeId);
    }

    @Override
    public void release() {
        for (AbstractNode node : this.nodeGroup) {
            node.getChannel().close();
        }
    }

}
