package in.xnnyygn.xraft.core.rpc.nio;

import io.netty.channel.Channel;

public class ConnectedChannelWriter implements ChannelWriter {

    private final Channel channel;

    ConnectedChannelWriter(Channel channel) {
        this.channel = channel;
    }

    @Override
    public State getState() {
        return State.CONNECTED;
    }

    @Override
    public void write(Object message) {
        this.channel.writeAndFlush(message);
    }

    @Override
    public void close() {
        try {
            this.channel.close().sync();
        } catch (InterruptedException ignored) {
        }
    }

}
