package in.xnnyygn.xraft.core.rpc.nio;

public interface NioChannelContext {

    void registerNioChannel(NioChannel channel);

    void closeNioChannel(NioChannel channel);

}
