package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.kvstore.message.CommandRequest;
import in.xnnyygn.xraft.kvstore.message.GetCommand;
import in.xnnyygn.xraft.kvstore.message.GetCommandResponse;
import in.xnnyygn.xraft.kvstore.message.SetCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ServiceHandler extends ChannelInboundHandlerAdapter {

    private final Service service;

    public ServiceHandler(Service service) {
        this.service = service;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof GetCommand) {
            service.get(new CommandRequest<>((GetCommand) msg, ctx.channel()));
        } else if (msg instanceof SetCommand) {
            service.set(new CommandRequest<>((SetCommand) msg, ctx.channel()));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }

}
