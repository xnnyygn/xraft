package in.xnnyygn.xraft.rpc;

import in.xnnyygn.xraft.server.ServerId;

public interface ChannelListener {

    void receive(Object payload, ServerId senderId);

}
