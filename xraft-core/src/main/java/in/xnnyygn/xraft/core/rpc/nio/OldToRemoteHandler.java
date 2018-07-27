package in.xnnyygn.xraft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeId;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class OldToRemoteHandler extends AbstractHandler {

    private static final Logger logger = LoggerFactory.getLogger(OldToRemoteHandler.class);
    private final NodeId selfNodeId;
    private final Object firstMessage;

    OldToRemoteHandler(EventBus eventBus, NodeId remoteId, OutboundChannel channel, NodeId selfNodeId, Object firstMessage) {
        super(eventBus);
        this.remoteId = remoteId;
        this.channel = channel;

        this.selfNodeId = selfNodeId;
        this.firstMessage = firstMessage;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.write(this.selfNodeId);
        ctx.channel().writeAndFlush(firstMessage);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logger.debug("receive {} from {}", msg, this.remoteId);
        super.channelRead(ctx, msg);
    }
}
