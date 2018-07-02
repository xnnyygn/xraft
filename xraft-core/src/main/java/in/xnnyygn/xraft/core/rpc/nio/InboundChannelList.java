package in.xnnyygn.xraft.core.rpc.nio;

import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class InboundChannelList {

    private static final Logger logger = LoggerFactory.getLogger(InboundChannelList.class);
    private final List<InboundChannel> channels = new CopyOnWriteArrayList<>();

    public void add(InboundChannel channel) {
        logger.info("channel INBOUND-{} connected", channel.remoteId);
        channel.delegate.closeFuture().addListener((ChannelFutureListener) future -> {
            logger.info("channel INBOUND-{} disconnected", channel.remoteId);
            remove(channel);
        });
    }

    private void remove(InboundChannel channel) {
        this.channels.remove(channel);
    }

    void closeAll() {
        for (InboundChannel channel : this.channels) {
            channel.close();
        }
    }

}
