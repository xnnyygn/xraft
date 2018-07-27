package in.xnnyygn.xraft.core.rpc.nio;

import in.xnnyygn.xraft.core.node.NodeId;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

class InboundChannelGroup {

    private static final Logger logger = LoggerFactory.getLogger(InboundChannelGroup.class);
    private final List<NioChannel> channels = new CopyOnWriteArrayList<>();

    public void add(NodeId remoteId, NioChannel channel) {
        logger.info("channel INBOUND-{} connected", remoteId);
        channel.getDelegate().closeFuture().addListener((ChannelFutureListener) future -> {
            logger.info("channel INBOUND-{} disconnected", remoteId);
            remove(channel);
        });
    }

    private void remove(NioChannel channel) {
        this.channels.remove(channel);
    }

    void closeAll() {
        logger.info("close all inbound channels");
        for (NioChannel channel : this.channels) {
            channel.close();
        }
    }

}
