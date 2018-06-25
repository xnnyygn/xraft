package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeId;

public interface Connector {

    void sendRpc(Object rpc);

    void sendRpc(Object rpc, NodeId destinationNodeId);

    void sendResult(Object result, NodeId destinationNodeId);

    void sendAppendEntriesResult(AppendEntriesResult result, NodeId destinationNodeId, AppendEntriesRpc rpc);

    void release();

}
