package in.xnnyygn.xraft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.DirectionalChannel;
import io.netty.channel.ChannelHandlerContext;

public class ToRemoteHandler extends AbstractHandler {

    public ToRemoteHandler(NodeId remoteId, EventBus eventBus, NioChannelContext nioChannelContext) {
        super(eventBus, nioChannelContext);
        this.remoteId = remoteId;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.setNioChannel(ctx.channel(), DirectionalChannel.Direction.OUTBOUND);
    }

}
