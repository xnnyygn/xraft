package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.AbstractNode;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractConnector implements Connector {

    private static final Logger logger = LoggerFactory.getLogger(AbstractConnector.class);
    protected final NodeGroup nodeGroup;
    protected final NodeId selfNodeId;

    public AbstractConnector(NodeGroup nodeGroup, NodeId selfNodeId) {
        this.nodeGroup = nodeGroup;
        this.selfNodeId = selfNodeId;
    }

    @Override
    public void initialize() {
    }

    @Override
    public void sendRequestVote(RequestVoteRpc rpc) {
        for (AbstractNode node : this.nodeGroup) {
            if (node.getId() != this.selfNodeId) {
                logger.debug("send {} to {}", rpc, node.getId());
                try {
                    getChannel(node).writeRequestVoteRpc(rpc, this.selfNodeId);
                } catch (Exception e) {
                    logger.warn(e.getMessage());
                }
            }
        }
    }

    @Override
    public void replyRequestVote(RequestVoteResult result, RequestVoteRpcMessage rpcMessage) {
        logger.debug("reply {} to {}", result, rpcMessage.getSourceNodeId());
        try {
            getChannel(rpcMessage).writeRequestVoteResult(result, this.selfNodeId, rpcMessage.get());
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
    }

    @Override
    public void sendAppendEntries(AppendEntriesRpc rpc, NodeId destinationNodeId) {
        logger.debug("send {} to {}", rpc, destinationNodeId);
        AbstractNode node = this.nodeGroup.find(destinationNodeId);
        try {
            getChannel(node).writeAppendEntriesRpc(rpc, this.selfNodeId);
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
    }

    @Override
    public void replyAppendEntries(AppendEntriesResult result, AppendEntriesRpcMessage rpcMessage) {
        logger.debug("reply {} to {}", result, rpcMessage.getSourceNodeId());
        try {
            getChannel(rpcMessage).writeAppendEntriesResult(result, this.selfNodeId, rpcMessage.get());
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
    }

    protected abstract Channel getChannel(AbstractNode node);

    protected <T> Channel getChannel(AbstractRpcMessage<T> rpcMessage) {
        return rpcMessage.getChannel();
    }

    @Override
    public void resetChannels() {
    }

    @Override
    public void release() {
    }

}
