package in.xnnyygn.xraft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.ChannelException;
import in.xnnyygn.xraft.core.rpc.Endpoint;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

@Deprecated
public class DisconnectedChannelWriter implements ChannelWriter {

    private final NioEventLoopGroup workerGroup;
    private final EventBus eventBus;
    private final NodeId remoteId;
    private final NodeId selfNodeId;
    private final Endpoint endpoint;
    private final OutboundChannel channel;

    DisconnectedChannelWriter(NioEventLoopGroup workerGroup, EventBus eventBus, NodeId remoteId, NodeId selfNodeId,
                              Endpoint endpoint, OutboundChannel channel) {
        this.workerGroup = workerGroup;
        this.eventBus = eventBus;
        this.remoteId = remoteId;
        this.selfNodeId = selfNodeId;
        this.endpoint = endpoint;
        this.channel = channel;
    }

    @Override
    public State getState() {
        return State.DISCONNECTED;
    }

    @Override
    public void write(Object message) {
        Bootstrap bootstrap = new Bootstrap()
                .group(this.workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new Decoder());
                        pipeline.addLast(new Encoder());
                        pipeline.addLast(new OldToRemoteHandler(eventBus, remoteId, channel, selfNodeId, message));
                    }
                });
        try {
            ChannelFuture connectFuture = bootstrap.connect(endpoint.getHost(), endpoint.getPort()).sync();
            if (connectFuture.isSuccess()) {
                Channel channel = connectFuture.channel();
                this.channel.setChannelWriter(new ConnectedChannelWriter(channel));
                channel.closeFuture().addListener((ChannelFutureListener) closeFuture ->
                        DisconnectedChannelWriter.this.channel.setChannelWriter(DisconnectedChannelWriter.this)
                );
            }
        } catch (InterruptedException e) {
            throw new ChannelException(e);
        }
    }

    @Override
    public void close() {
    }

}
