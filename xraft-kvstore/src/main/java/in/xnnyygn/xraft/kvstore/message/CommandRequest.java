package in.xnnyygn.xraft.kvstore.message;

import io.netty.channel.Channel;

public class CommandRequest<T> {

    private final T command;
    private final Channel channel;

    public CommandRequest(T command, Channel channel) {
        this.command = command;
        this.channel = channel;
    }

    public void reply(Object response) {
        this.channel.writeAndFlush(response);
    }

    public T getCommand() {
        return command;
    }

}
