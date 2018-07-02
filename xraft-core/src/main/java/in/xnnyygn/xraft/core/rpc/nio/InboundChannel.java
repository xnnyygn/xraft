package in.xnnyygn.xraft.core.rpc.nio;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.Channel;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResult;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteResult;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpc;

public class InboundChannel implements Channel {

    final io.netty.channel.Channel delegate;
    final NodeId remoteId;

    InboundChannel(io.netty.channel.Channel delegate, NodeId remoteId) {
        this.delegate = delegate;
        this.remoteId = remoteId;
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

    private void write(Object message) {
        this.delegate.writeAndFlush(message);
    }

    @Override
    public void close() {
        try {
            this.delegate.close().sync();
        } catch (InterruptedException ignored) {
        }
    }

}
