package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeId;

public class MockConnector implements Connector {

    private Object rpc;
    private Object result;
    private NodeId destinationNodeId;

    @Override
    public void sendRpc(Object rpc) {
        this.rpc = rpc;
    }

    @Override
    public void sendRpc(Object rpc, NodeId destinationNodeId) {
        this.rpc = rpc;
        this.destinationNodeId = destinationNodeId;
    }

    @Override
    public void sendResult(Object result, NodeId destinationNodeId) {
        this.result = result;
        this.destinationNodeId = destinationNodeId;
    }

    @Override
    public void sendAppendEntriesResult(AppendEntriesResult result, NodeId destinationNodeId, AppendEntriesRpc rpc) {
        this.result = result;
        this.destinationNodeId = destinationNodeId;
    }

    public Object getRpc() {
        return rpc;
    }

    public Object getResult() {
        return result;
    }

    public NodeId getDestinationNodeId() {
        return destinationNodeId;
    }

    @Override
    public void release() {
    }

}
