package in.xnnyygn.xraft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.DirectionalChannel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FromRemoteHandler extends AbstractHandler {

    private static final Logger logger = LoggerFactory.getLogger(FromRemoteHandler.class);

    public FromRemoteHandler(EventBus eventBus, NioChannelContext nioChannelContext) {
        super(eventBus, nioChannelContext);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof NodeId) {
            this.remoteId = (NodeId) msg;
            logger.debug("receive remote node id {}", this.remoteId);
            this.setNioChannel(ctx.channel(), DirectionalChannel.Direction.INBOUND);
            return;
        }

        super.channelRead(ctx, msg);
    }

}
