package in.xnnyygn.xraft.rpc;

import in.xnnyygn.xraft.server.ServerId;

public interface Channel {

    void write(Object payload, ServerId senderId);

}
