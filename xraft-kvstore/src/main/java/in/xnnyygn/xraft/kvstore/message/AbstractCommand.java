package in.xnnyygn.xraft.kvstore.message;

import io.netty.channel.Channel;

// TODO rename to message
public abstract class AbstractCommand {

    protected final Channel channel;

    public AbstractCommand(Channel channel) {
        this.channel = channel;
    }

    public void reply(Object response) {
        this.channel.writeAndFlush(response);
    }

}
