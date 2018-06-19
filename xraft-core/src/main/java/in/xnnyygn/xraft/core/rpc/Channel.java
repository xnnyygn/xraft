package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeId;

public interface Channel {

    void send(Object payload, NodeId senderId);

    void close() throws Exception;

}
