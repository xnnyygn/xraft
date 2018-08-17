package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;

import java.util.Collection;

// TODO add doc
public interface Connector {

    void initialize();

    void sendRequestVote(RequestVoteRpc rpc, Collection<NodeEndpoint> destinationEndpoints);

    void replyRequestVote(RequestVoteResult result, RequestVoteRpcMessage rpcMessage);

    void sendAppendEntries(AppendEntriesRpc rpc, NodeEndpoint destinationEndpoint);

    void replyAppendEntries(AppendEntriesResult result, AppendEntriesRpcMessage rpcMessage);

    void sendInstallSnapshot(InstallSnapshotRpc rpc, NodeEndpoint destinationEndpoint);

    void replyInstallSnapshot(InstallSnapshotResult result, InstallSnapshotRpcMessage rpcMessage);

    void resetChannels();

    void close();

}
