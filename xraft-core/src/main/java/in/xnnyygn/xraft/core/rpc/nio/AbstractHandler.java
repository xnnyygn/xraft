package in.xnnyygn.xraft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.DirectionalChannel;
import in.xnnyygn.xraft.core.rpc.message.*;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractHandler.class);
    protected final EventBus eventBus;
    protected final NioChannelContext nioChannelContext;

    protected NodeId remoteId;
    protected AppendEntriesRpc lastAppendEntriesRpc;
    protected NioChannel nioChannel;

    public AbstractHandler(EventBus eventBus, NioChannelContext nioChannelContext) {
        this.eventBus = eventBus;
        this.nioChannelContext = nioChannelContext;
    }

    protected void setNioChannel(Channel channel, DirectionalChannel.Direction direction) {
        this.nioChannel = new NioChannel(channel, this.remoteId, direction);
        this.nioChannelContext.registerNioChannel(this.nioChannel);
        channel.closeFuture().addListener((ChannelFutureListener) future -> nioChannelContext.closeNioChannel(nioChannel));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert this.remoteId != null;

        logger.debug("receive {} from {}", msg, this.remoteId);
        if (msg instanceof RequestVoteRpc) {
            RequestVoteRpc rpc = (RequestVoteRpc) msg;
            this.eventBus.post(new RequestVoteRpcMessage(rpc, this.remoteId, this.nioChannel));
        } else if (msg instanceof RequestVoteResult) {
            this.eventBus.post(msg);
        } else if (msg instanceof AppendEntriesRpc) {
            AppendEntriesRpc rpc = (AppendEntriesRpc) msg;
            this.eventBus.post(new AppendEntriesRpcMessage(rpc, this.remoteId, this.nioChannel));
        } else if (msg instanceof AppendEntriesResult) {
            AppendEntriesResult result = (AppendEntriesResult) msg;
            assert this.lastAppendEntriesRpc != null;
            this.eventBus.post(new AppendEntriesResultMessage(result, this.remoteId, this.lastAppendEntriesRpc));
            this.lastAppendEntriesRpc = null;
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof AppendEntriesRpc) {
            this.lastAppendEntriesRpc = (AppendEntriesRpc) msg;
        }
        super.write(ctx, msg, promise);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn(cause.getMessage());
        ctx.close();
    }

}
