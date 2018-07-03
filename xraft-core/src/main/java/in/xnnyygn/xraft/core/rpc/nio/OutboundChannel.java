package in.xnnyygn.xraft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.Channel;
import in.xnnyygn.xraft.core.rpc.message.*;
import in.xnnyygn.xraft.core.rpc.socket.SocketEndpoint;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO AbstractNioChannel
public class OutboundChannel implements Channel {

    private static final Logger logger = LoggerFactory.getLogger(OutboundChannel.class);
    private final NodeId remoteId;
    private volatile ChannelWriter channelWriter;

    OutboundChannel(NioEventLoopGroup workerGroup, EventBus eventBus, NodeId remoteId,
                    NodeId selfNodeId, SocketEndpoint endpoint) {
        this.remoteId = remoteId;
        this.channelWriter = new DisconnectedChannelWriter(workerGroup, eventBus, remoteId, selfNodeId, endpoint, this);
    }

    @Override
    public void writeRequestVoteRpc(RequestVoteRpc rpc, NodeId senderId) {
        this.write(rpc);
    }

    @Override
    public void writeRequestVoteResult(RequestVoteResult result, NodeId senderId, RequestVoteRpc rpc) {
        this.write(result);
    }

    @Override
    public void writeAppendEntriesRpc(AppendEntriesRpc rpc, NodeId senderId) {
        this.write(rpc);
    }

    @Override
    public void writeAppendEntriesResult(AppendEntriesResult result, NodeId senderId, AppendEntriesRpc rpc) {
        this.write(result);
    }

    @Override
    public void writeInstallSnapshotResult(InstallSnapshotResult result, NodeId senderId, InstallSnapshotRpc rpc) {
        this.write(result);
    }

    @Override
    public void writeInstallSnapshotRpc(InstallSnapshotRpc rpc, NodeId senderId) {
        this.write(rpc);
    }

    private void write(Object message) {
        this.channelWriter.write(message);
    }

    void setChannelWriter(ChannelWriter channelWriter) {
        logger.info("channel OUTBOUND-{} {}", this.remoteId, channelWriter.getState().name().toLowerCase());
        this.channelWriter = channelWriter;
    }

    @Override
    public void close() {
        this.channelWriter.close();
    }

}
