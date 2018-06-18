package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.server.ServerId;

public interface Channel {

    void write(Object payload, ServerId senderId);

    void close() throws Exception;

}
