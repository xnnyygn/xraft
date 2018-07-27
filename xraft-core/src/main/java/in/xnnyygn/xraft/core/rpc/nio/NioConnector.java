package in.xnnyygn.xraft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeGroup;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.Channel;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.rpc.message.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NioConnector implements Connector {

    private static final Logger logger = LoggerFactory.getLogger(NioConnector.class);
    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final NioEventLoopGroup workerGroup = new NioEventLoopGroup(4);
    private final InboundChannelGroup inboundChannelGroup = new InboundChannelGroup();
    private final OutboundChannelGroup outboundChannelGroup;
    private final NodeGroup nodeGroup;
    private final NodeId selfNodeId;
    private final EventBus eventBus;
    private final int port;

    public NioConnector(NodeGroup nodeGroup, NodeId selfNodeId, EventBus eventBus, int port) {
        this.nodeGroup = nodeGroup;
        this.selfNodeId = selfNodeId;
        this.eventBus = eventBus;
        this.port = port;
        this.outboundChannelGroup = new OutboundChannelGroup(workerGroup, eventBus, selfNodeId);
    }

    // TODO open after
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
                        pipeline.addLast(new FromRemoteHandler(eventBus, inboundChannelGroup));
                    }
                });
        logger.debug("start acceptor at port {}", port);
        try {
            serverBootstrap.bind(port).sync();
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void sendRequestVote(RequestVoteRpc rpc) {
        for (NodeConfig config : nodeGroup) {
            if (config.getId().equals(selfNodeId)) continue;

            logger.debug("send {} to {}", rpc, config.getId());
            try {
                getChannel(config).writeRequestVoteRpc(rpc);
            } catch (Exception e) {
                logger.warn("failed to send", e);
            }
        }
    }

    @Override
    public void replyRequestVote(RequestVoteResult result, RequestVoteRpcMessage rpcMessage) {
        logger.debug("reply {} to {}", result, rpcMessage.getSourceNodeId());
        try {
            rpcMessage.getChannel().writeRequestVoteResult(result);
        } catch (Exception e) {
            logger.warn("failed to reply", e);
        }
    }

    @Override
    public void sendAppendEntries(AppendEntriesRpc rpc, NodeId destinationNodeId) {
        logger.debug("send {} to {}", rpc, destinationNodeId);
        try {
            getChannel(destinationNodeId).writeAppendEntriesRpc(rpc);
        } catch (Exception e) {
            logger.warn("failed to send", e);
        }
    }

    @Override
    public void replyAppendEntries(AppendEntriesResult result, AppendEntriesRpcMessage rpcMessage) {
        logger.debug("reply {} to {}", result, rpcMessage.getSourceNodeId());
        try {
            rpcMessage.getChannel().writeAppendEntriesResult(result);
        } catch (Exception e) {
            logger.warn("failed to reply", e);
        }
    }

    @Override
    public void sendInstallSnapshot(InstallSnapshotRpc rpc, NodeId destinationNodeId) {
        logger.debug("send {} to {}", rpc, destinationNodeId);
        try {
            getChannel(destinationNodeId).writeInstallSnapshotRpc(rpc);
        } catch (Exception e) {
            logger.warn("failed to send", e);
        }
    }

    @Override
    public void replyInstallSnapshot(InstallSnapshotResult result, InstallSnapshotRpcMessage rpcMessage) {
        logger.debug("reply {} to {}", result, rpcMessage.getSourceNodeId());
        try {
            rpcMessage.getChannel().writeInstallSnapshotResult(result);
        } catch (Exception e) {
            logger.warn("failed to reply", e);
        }
    }

    @Override
    public void resetChannels() {
        inboundChannelGroup.closeAll();
    }

    private Channel getChannel(NodeId nodeId) {
        return getChannel(nodeGroup.find(nodeId));
    }

    private Channel getChannel(NodeConfig config) {
        return outboundChannelGroup.getOrConnect(config.getId(), config.getEndpoint());
    }

    @Override
    public void close() {
        logger.debug("close connector");
        inboundChannelGroup.closeAll();
        outboundChannelGroup.closeAll();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

}
