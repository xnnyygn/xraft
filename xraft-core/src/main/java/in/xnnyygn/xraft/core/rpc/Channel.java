package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.server.ServerId;

public interface Channel {

    // TODO rename to send
    void write(Object payload, ServerId senderId);

    void close() throws Exception;

}
