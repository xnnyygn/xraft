package in.xnnyygn.xraft.core.rpc.nio;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.Channel;
import in.xnnyygn.xraft.core.rpc.ChannelConnectException;
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
    private final NioEventLoopGroup bossNioEventLoopGroup = new NioEventLoopGroup(1);
    private final NioConnectorContext context;
    private final InboundChannelGroup inboundChannelGroup = new InboundChannelGroup();
    private final OutboundChannelGroup outboundChannelGroup;

    public NioConnector(NioConnectorContext context) {
        this.context = context;
        this.outboundChannelGroup = new OutboundChannelGroup(context.workerNioEventLoopGroup(), context.eventBus(), context.selfNodeId());
    }

    @Override
    public void initialize() {
        ServerBootstrap serverBootstrap = new ServerBootstrap()
                .group(bossNioEventLoopGroup, context.workerNioEventLoopGroup())
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new Decoder());
                        pipeline.addLast(new Encoder());
                        pipeline.addLast(new FromRemoteHandler(context.eventBus(), inboundChannelGroup));
                    }
                });
        logger.debug("connector listen on port {}", context.port());
        try {
            serverBootstrap.bind(context.port()).sync();
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void sendRequestVote(RequestVoteRpc rpc) {
        for (NodeEndpoint endpoint : context.nodeGroup().getNodeEndpointsOfMajor()) {
            if (endpoint.getId().equals(context.selfNodeId())) {
                continue;
            }
            logger.debug("send {} to node {}", rpc, endpoint.getId());
            try {
                getChannel(endpoint).writeRequestVoteRpc(rpc);
            } catch (Exception e) {
                logException(e);
            }
        }
    }

    private void logException(Exception e) {
        if (e instanceof ChannelConnectException) {
            logger.warn(e.getMessage());
        } else {
            logger.warn("failed to process channel", e);
        }
    }

    @Override
    public void replyRequestVote(RequestVoteResult result, RequestVoteRpcMessage rpcMessage) {
        logger.debug("reply {} to node {}", result, rpcMessage.getSourceNodeId());
        try {
            rpcMessage.getChannel().writeRequestVoteResult(result);
        } catch (Exception e) {
            logException(e);
        }
    }

    @Override
    public void sendAppendEntries(AppendEntriesRpc rpc, NodeId destinationNodeId) {
        logger.debug("send {} to node {}", rpc, destinationNodeId);
        try {
            getChannel(destinationNodeId).writeAppendEntriesRpc(rpc);
        } catch (Exception e) {
            logException(e);
        }
    }

    @Override
    public void replyAppendEntries(AppendEntriesResult result, AppendEntriesRpcMessage rpcMessage) {
        logger.debug("reply {} to node {}", result, rpcMessage.getSourceNodeId());
        try {
            rpcMessage.getChannel().writeAppendEntriesResult(result);
        } catch (Exception e) {
            logException(e);
        }
    }

    @Override
    public void sendInstallSnapshot(InstallSnapshotRpc rpc, NodeId destinationNodeId) {
        logger.debug("send {} to node {}", rpc, destinationNodeId);
        try {
            getChannel(destinationNodeId).writeInstallSnapshotRpc(rpc);
        } catch (Exception e) {
            logException(e);
        }
    }

    @Override
    public void replyInstallSnapshot(InstallSnapshotResult result, InstallSnapshotRpcMessage rpcMessage) {
        logger.debug("reply {} to node {}", result, rpcMessage.getSourceNodeId());
        try {
            rpcMessage.getChannel().writeInstallSnapshotResult(result);
        } catch (Exception e) {
            logException(e);
        }
    }

    @Override
    public void resetChannels() {
        inboundChannelGroup.closeAll();
    }

    private Channel getChannel(NodeId nodeId) {
        return getChannel(context.nodeGroup().findEndpoint(nodeId));
    }

    private Channel getChannel(NodeEndpoint endpoint) {
        return outboundChannelGroup.getOrConnect(endpoint.getId(), endpoint.getAddress());
    }

    @Override
    public void close() {
        logger.debug("close connector");
        inboundChannelGroup.closeAll();
        outboundChannelGroup.closeAll();
        bossNioEventLoopGroup.shutdownGracefully();
        if (!context.workerGroupShared()) {
            context.workerNioEventLoopGroup().shutdownGracefully();
        }
    }

}
