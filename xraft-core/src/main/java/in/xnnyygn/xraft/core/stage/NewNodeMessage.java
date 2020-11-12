package in.xnnyygn.xraft.core.stage;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import io.netty.channel.Channel;

public class NewNodeMessage {
    public static final int STATUS_NODE_DUPLICATED = 1;
    public static final int STATUS_TIMEOUT = 2;

    final NodeEndpoint endpoint;
    private final Channel clientChannel;

    public NewNodeMessage(NodeEndpoint endpoint, Channel clientChannel) {
        this.endpoint = endpoint;
        this.clientChannel = clientChannel;
    }

    public NodeId getNodeId() {
        return endpoint.getId();
    }

    public void reply(int statusCode) {
        clientChannel.writeAndFlush(statusCode);
    }
}
