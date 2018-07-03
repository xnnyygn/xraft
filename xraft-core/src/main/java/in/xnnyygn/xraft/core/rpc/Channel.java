package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;

public interface Channel {

    void writeRequestVoteRpc(RequestVoteRpc rpc, NodeId senderId);

    void writeRequestVoteResult(RequestVoteResult result, NodeId senderId, RequestVoteRpc rpc);

    void writeAppendEntriesRpc(AppendEntriesRpc rpc, NodeId senderId);

    void writeAppendEntriesResult(AppendEntriesResult result, NodeId senderId, AppendEntriesRpc rpc);

    void writeInstallSnapshotRpc(InstallSnapshotRpc rpc, NodeId senderId);

    void writeInstallSnapshotResult(InstallSnapshotResult result, NodeId senderId, InstallSnapshotRpc rpc);

    void close();

}
