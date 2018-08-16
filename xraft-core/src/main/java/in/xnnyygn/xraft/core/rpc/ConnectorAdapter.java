package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;

public abstract class ConnectorAdapter implements Connector {

    @Override
    public void initialize() {
    }

    @Override
    public void sendRequestVote(RequestVoteRpc rpc) {

    }

    @Override
    public void replyRequestVote(RequestVoteResult result, RequestVoteRpcMessage rpcMessage) {

    }

    @Override
    public void sendAppendEntries(AppendEntriesRpc rpc, NodeId destinationNodeId) {

    }

    @Override
    public void sendAppendEntries(AppendEntriesRpc rpc, NodeEndpoint destinationEndpoint) {

    }

    @Override
    public void replyAppendEntries(AppendEntriesResult result, AppendEntriesRpcMessage rpcMessage) {

    }

    @Override
    public void sendInstallSnapshot(InstallSnapshotRpc rpc, NodeId destinationNodeId) {

    }

    @Override
    public void sendInstallSnapshot(InstallSnapshotRpc rpc, NodeEndpoint destinationEndpoint) {

    }

    @Override
    public void replyInstallSnapshot(InstallSnapshotResult result, InstallSnapshotRpcMessage rpcMessage) {

    }

    @Override
    public void resetChannels() {
    }

    @Override
    public void close() {
    }

}
