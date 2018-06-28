package in.xnnyygn.xraft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.AbstractNode;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.*;
import in.xnnyygn.xraft.core.rpc.Channel;
import in.xnnyygn.xraft.core.rpc.ChannelException;
import in.xnnyygn.xraft.core.rpc.socket.SocketEndpoint;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NioConnector extends AbstractConnector implements NioChannelContext {

    private static final Logger logger = LoggerFactory.getLogger(NioConnector.class);
    private final DirectionalChannelRegister channelRegister = new DirectionalChannelRegister();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "nio-connector"));
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup(4);

    private final EventBus eventBus;
    private final int port;

    public NioConnector(NodeGroup nodeGroup, NodeId selfNodeId, EventBus eventBus, int port) {
        super(nodeGroup, selfNodeId);
        this.eventBus = eventBus;
        this.port = port;
    }

    @Override
    public void initialize() {
        ServerBootstrap serverBootstrap = new ServerBootstrap()
                .group(this.bossGroup, this.workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new Decoder());
                        pipeline.addLast(new Encoder());
                        pipeline.addLast(new FromRemoteHandler(eventBus, NioConnector.this));
                    }
                });
        logger.debug("start acceptor at port {}", this.port);
        try {
            serverBootstrap.bind(this.port).sync();
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    protected Channel getChannel(AbstractNode node) {
        NodeId nodeId = node.getId();
        DirectionalChannel channel = this.channelRegister.find(nodeId);
        if (channel != null) return channel;

        Endpoint endpoint = node.getEndpoint();
        if (!(endpoint instanceof SocketEndpoint)) {
            throw new IllegalArgumentException("expect socket endpoint");
        }

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
                        pipeline.addLast(new ToRemoteHandler(nodeId, eventBus, NioConnector.this));
                    }
                });
        SocketEndpoint socketEndpoint = (SocketEndpoint) endpoint;

        try {
            ChannelFuture future = bootstrap.connect(socketEndpoint.getHost(), socketEndpoint.getPort()).sync();
            future.channel().writeAndFlush(this.selfNodeId).sync();
            channel = new NioChannel(future.channel(), nodeId, DirectionalChannel.Direction.OUTBOUND);
        } catch (InterruptedException e) {
            throw new ChannelException(e);
        }
        return channel;
    }

    @Override
    public void registerNioChannel(NioChannel channel) {
        this.executorService.submit(() -> {
            this.channelRegister.register(channel);
        });
    }

    @Override
    public void closeNioChannel(NioChannel channel) {
        this.executorService.submit(() -> {
            this.channelRegister.remove(channel);
        });
    }

    @Override
    public void resetChannels() {
        this.channelRegister.closeInboundChannels();
    }

    @Override
    public void release() {
        this.channelRegister.release();

        this.workerGroup.shutdownGracefully();
        this.bossGroup.shutdownGracefully();

        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(1L, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }
}
