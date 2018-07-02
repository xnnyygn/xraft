package in.xnnyygn.xraft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeId;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FromRemoteHandler extends AbstractHandler {

    private static final Logger logger = LoggerFactory.getLogger(FromRemoteHandler.class);
    private final InboundChannelList channelList;

    FromRemoteHandler(EventBus eventBus, InboundChannelList channelList) {
        super(eventBus);
        this.channelList = channelList;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof NodeId) {
            this.remoteId = (NodeId) msg;
            InboundChannel inboundChannel = new InboundChannel(ctx.channel(), this.remoteId);
            this.channel = inboundChannel;
            this.channelList.add(inboundChannel);
            return;
        }

        logger.debug("receive {} from {}", msg, this.remoteId);
        super.channelRead(ctx, msg);
    }

}
