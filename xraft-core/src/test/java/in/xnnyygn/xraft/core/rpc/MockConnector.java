package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;

import java.util.Set;

public class MockConnector implements Connector {

    private Object rpc;
    private Object result;
    private NodeId destinationNodeId;

    @Override
    public void initialize() {
    }

    @Override
    public void resetChannels() {
    }

    @Override
    public void sendRequestVote(RequestVoteRpc rpc) {
        this.rpc = rpc;
    }

    @Override
    public void replyRequestVote(RequestVoteResult result, RequestVoteRpcMessage rpcMessage) {
        this.result = result;
        this.destinationNodeId = rpcMessage.getSourceNodeId();
    }

    @Override
    public void sendAppendEntries(AppendEntriesRpc rpc, NodeId destinationNodeId) {
        this.rpc = rpc;
        this.destinationNodeId = destinationNodeId;
    }

    @Override
    public void replyAppendEntries(AppendEntriesResult result, AppendEntriesRpcMessage rpcMessage) {
        this.result = result;
        this.destinationNodeId = rpcMessage.getSourceNodeId();
    }

    @Override
    public void sendInstallSnapshot(InstallSnapshotRpc rpc, NodeId destinationNodeId) {
        this.rpc = rpc;
        this.destinationNodeId = destinationNodeId;
    }

    @Override
    public void replyInstallSnapshot(InstallSnapshotResult result, InstallSnapshotRpcMessage rpcMessage) {
        this.result = result;
        this.destinationNodeId = rpcMessage.getSourceNodeId();
    }

//    @Override
//    public void applyNodeConfigs(Set<NodeConfig> nodeConfigs, NodeId selfNodeId) {
//    }

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
    public void close() {
    }

}
