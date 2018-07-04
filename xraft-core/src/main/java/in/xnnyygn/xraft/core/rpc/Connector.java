package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;

public interface Connector {

    void initialize();

    void sendRequestVote(RequestVoteRpc rpc);

    void replyRequestVote(RequestVoteResult result, RequestVoteRpcMessage rpcMessage);

    void sendAppendEntries(AppendEntriesRpc rpc, NodeId destinationNodeId);

    void replyAppendEntries(AppendEntriesResult result, AppendEntriesRpcMessage rpcMessage);

    void sendInstallSnapshot(InstallSnapshotRpc rpc, NodeId destinationNodeId);

    void replyInstallSnapshot(InstallSnapshotResult result, InstallSnapshotRpcMessage rpcMessage);

    void resetChannels();

    void release();

}
