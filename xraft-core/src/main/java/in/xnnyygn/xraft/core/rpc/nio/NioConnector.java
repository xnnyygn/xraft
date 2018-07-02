package in.xnnyygn.xraft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.AbstractNode;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.AbstractConnector;
import in.xnnyygn.xraft.core.rpc.Channel;
import in.xnnyygn.xraft.core.rpc.socket.SocketEndpoint;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class NioConnector extends AbstractConnector {

    private static final Logger logger = LoggerFactory.getLogger(NioConnector.class);
    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final NioEventLoopGroup workerGroup = new NioEventLoopGroup(4);
    private final InboundChannelList inboundChannels = new InboundChannelList();
    private final Map<NodeId, OutboundChannel> outboundChannels = new HashMap<>();
    private final EventBus eventBus;
    private final int port;

    public NioConnector(NodeGroup nodeGroup, NodeId selfNodeId, EventBus eventBus, int port) {
        super(nodeGroup, selfNodeId);
        this.eventBus = eventBus;
        this.port = port;
    }

    @Override
    public void initialize() {
        for (AbstractNode node : this.nodeGroup) {
            if (node.getId() != this.selfNodeId) {
                this.outboundChannels.put(node.getId(), new OutboundChannel(
                        this.workerGroup, this.eventBus, node.getId(), this.selfNodeId, (SocketEndpoint) node.getEndpoint())
                );
            }
        }

        ServerBootstrap serverBootstrap = new ServerBootstrap()
                .group(this.bossGroup, this.workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new Decoder());
                        pipeline.addLast(new Encoder());
                        pipeline.addLast(new FromRemoteHandler(eventBus, inboundChannels));
                    }
                });
        logger.debug("start acceptor at port {}", this.port);
        try {
            serverBootstrap.bind(this.port).sync();
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void resetChannels() {
        logger.debug("close inbound channels");
        this.inboundChannels.closeAll();
    }

    @Override
    public void release() {
        logger.debug("close connector");

        logger.debug("close inbound channels");
        this.inboundChannels.closeAll();

        logger.debug("close outbound channels");
        for (OutboundChannel channel : outboundChannels.values()) {
            channel.close();
        }

        this.bossGroup.shutdownGracefully();
        this.workerGroup.shutdownGracefully();
    }

    @Override
    protected Channel getChannel(AbstractNode node) {
        return this.outboundChannels.get(node.getId());
    }

}
