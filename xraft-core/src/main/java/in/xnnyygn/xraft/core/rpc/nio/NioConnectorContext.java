package in.xnnyygn.xraft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import io.netty.channel.nio.NioEventLoopGroup;

public class NioConnectorContext {

    private boolean workerGroupShared = false;
    private NioEventLoopGroup workerNioEventLoopGroup;
    private NodeGroup nodeGroup;
    private NodeId selfNodeId;
    private EventBus eventBus;
    private int port;

    public boolean workerGroupShared() {
        return workerGroupShared;
    }

    public void setWorkerGroupShared(boolean workerGroupShared) {
        this.workerGroupShared = workerGroupShared;
    }

    public NioEventLoopGroup workerNioEventLoopGroup() {
        return workerNioEventLoopGroup;
    }

    public void setWorkerNioEventLoopGroup(NioEventLoopGroup workerNioEventLoopGroup) {
        this.workerNioEventLoopGroup = workerNioEventLoopGroup;
    }

    public NodeGroup nodeGroup() {
        return nodeGroup;
    }

    public void setNodeGroup(NodeGroup nodeGroup) {
        this.nodeGroup = nodeGroup;
    }

    public NodeId selfNodeId() {
        return selfNodeId;
    }

    public void setSelfNodeId(NodeId selfNodeId) {
        this.selfNodeId = selfNodeId;
    }

    public EventBus eventBus() {
        return eventBus;
    }

    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    public int port() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
