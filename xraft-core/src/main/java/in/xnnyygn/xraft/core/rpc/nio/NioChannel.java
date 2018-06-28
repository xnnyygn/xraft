package in.xnnyygn.xraft.core.rpc.nio;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.DirectionalChannel;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpc;
import io.netty.channel.Channel;

public class NioChannel implements DirectionalChannel {

    protected final Channel channel;
    protected final NodeId remoteId;
    protected final Direction direction;

    public NioChannel(Channel channel, NodeId remoteId, Direction direction) {
        this.channel = channel;
        this.remoteId = remoteId;
        this.direction = direction;
    }

    @Override
    public NodeId getRemoteId() {
        return this.remoteId;
    }

    @Override
    public Direction getDirection() {
        return this.direction;
    }

    @Override
    public void writeRequestVoteRpc(RequestVoteRpc rpc, NodeId senderId) {
        this.channel.writeAndFlush(rpc);
    }

    @Override
    public void writeRequestVoteResult(RequestVoteResult result, NodeId senderId, RequestVoteRpc rpc) {
        this.channel.writeAndFlush(result);
    }

    @Override
    public void writeAppendEntriesRpc(AppendEntriesRpc rpc, NodeId senderId) {
        this.channel.writeAndFlush(rpc);
    }

    @Override
    public void writeAppendEntriesResult(AppendEntriesResult result, NodeId senderId, AppendEntriesRpc rpc) {
        this.channel.writeAndFlush(result);
    }

    @Override
    public void close() {
        try {
            this.channel.close().sync();
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public String toString() {
        return "NioChannel{nio-channel-" + this.direction.name().toLowerCase() + '-' + this.remoteId + "}";
    }

}
