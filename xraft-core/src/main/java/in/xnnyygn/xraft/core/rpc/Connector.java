package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;

public interface Connector {

    void initialize();

    void sendRequestVote(RequestVoteRpc rpc);

    void replyRequestVote(RequestVoteResult result, RequestVoteRpcMessage rpcMessage);

    void sendAppendEntries(AppendEntriesRpc rpc, NodeId destinationNodeId);

    void sendAppendEntries(AppendEntriesRpc rpc, NodeEndpoint destinationEndpoint);

    void replyAppendEntries(AppendEntriesResult result, AppendEntriesRpcMessage rpcMessage);

    void sendInstallSnapshot(InstallSnapshotRpc rpc, NodeId destinationNodeId);

    void sendInstallSnapshot(InstallSnapshotRpc rpc, NodeEndpoint destinationEndpoint);

    void replyInstallSnapshot(InstallSnapshotResult result, InstallSnapshotRpcMessage rpcMessage);

    void resetChannels();

    void close();

}
