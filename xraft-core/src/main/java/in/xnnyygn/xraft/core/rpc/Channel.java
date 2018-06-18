package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeId;

public interface Channel {

    // TODO rename to send
    void write(Object payload, NodeId senderId);

    void close() throws Exception;

}
