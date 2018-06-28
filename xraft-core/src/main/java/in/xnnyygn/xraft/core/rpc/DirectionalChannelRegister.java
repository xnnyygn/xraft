package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DirectionalChannelRegister {

    private static final Logger logger = LoggerFactory.getLogger(DirectionalChannelRegister.class);
    private final Map<NodeId, DirectionalChannel> outboundChannels;
    private final Map<NodeId, DirectionalChannel> inboundChannels;

    public DirectionalChannelRegister() {
        this.outboundChannels = new HashMap<>();
        this.inboundChannels = new HashMap<>();
    }

    public void register(DirectionalChannel channel) {
        switch (channel.getDirection()) {
            case OUTBOUND:
                this.register(this.outboundChannels, channel);
                break;
            case INBOUND:
                this.register(this.inboundChannels, channel);
                break;
            default:
                throw new IllegalArgumentException("unexpected direction " + channel.getDirection());
        }
    }

    private void register(Map<NodeId, DirectionalChannel> map, DirectionalChannel channel) {
        assert channel != null;

        NodeId nodeId = channel.getRemoteId();
        Channel oldChannel = map.get(nodeId);
        if (oldChannel == channel) return;

        if (oldChannel != null) {
            oldChannel.close();
        }

        logger.debug("register {}", channel);
        map.put(nodeId, channel);
    }

    public DirectionalChannel find(NodeId nodeId) {
        DirectionalChannel channelToRemote = this.outboundChannels.get(nodeId);
        if (channelToRemote != null) return channelToRemote;

        return this.inboundChannels.get(nodeId);
    }

    public void closeInboundChannels() {
        logger.debug("close inbound channels");
        this.closeChannels(this.inboundChannels.values());
    }

    public void remove(DirectionalChannel channel) {
        logger.debug("remove {}", channel);
        switch (channel.getDirection()) {
            case OUTBOUND:
                this.outboundChannels.remove(channel.getRemoteId());
                break;
            case INBOUND:
                this.inboundChannels.remove(channel.getRemoteId());
                break;
            default:
                throw new IllegalArgumentException("unexpected direction " + channel.getDirection());
        }
    }

    public void release() {
        if (!this.outboundChannels.isEmpty()) {
            logger.debug("close outbound channels, count {}", this.outboundChannels.size());
            this.closeChannels(this.outboundChannels.values());
        }
        if (!this.inboundChannels.isEmpty()) {
            logger.debug("close inbound channels, count {}", this.inboundChannels.size());
            this.closeChannels(this.inboundChannels.values());
        }
    }

    private void closeChannels(Collection<DirectionalChannel> channels) {
        for (DirectionalChannel channel : channels) {
            channel.close();
        }
    }

}
