package in.xnnyygn.xraft.core.rpc.message;

import in.xnnyygn.xraft.core.node.NodeId;

public class InstallSnapshotResultMessage {

    private final InstallSnapshotResult result;
    private final NodeId sourceNodeId;
    private final InstallSnapshotRpc rpc;

    public InstallSnapshotResultMessage(InstallSnapshotResult result, NodeId sourceNodeId, InstallSnapshotRpc rpc) {
        this.result = result;
        this.sourceNodeId = sourceNodeId;
        this.rpc = rpc;
    }

    public InstallSnapshotResult get() {
        return result;
    }

    public NodeId getSourceNodeId() {
        return sourceNodeId;
    }

    public InstallSnapshotRpc getRpc() {
        return rpc;
    }

}
