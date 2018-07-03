package in.xnnyygn.xraft.core.rpc.embedded;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.*;
import in.xnnyygn.xraft.core.rpc.message.*;

public class EmbeddedChannel implements Channel {

    private final EventBus eventBus;

    public EmbeddedChannel(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public void writeRequestVoteRpc(RequestVoteRpc rpc, NodeId senderId) {
        this.eventBus.post(new RequestVoteRpcMessage(rpc, senderId, null));
    }

    @Override
    public void writeRequestVoteResult(RequestVoteResult result, NodeId senderId, RequestVoteRpc rpc) {
        this.eventBus.post(result);
    }

    @Override
    public void writeAppendEntriesRpc(AppendEntriesRpc rpc, NodeId senderId) {
        this.eventBus.post(new AppendEntriesRpcMessage(rpc, senderId, null));
    }

    @Override
    public void writeAppendEntriesResult(AppendEntriesResult result, NodeId senderId, AppendEntriesRpc rpc) {
        this.eventBus.post(new AppendEntriesResultMessage(result, senderId, rpc));
    }

    @Override
    public void writeInstallSnapshotRpc(InstallSnapshotRpc rpc, NodeId senderId) {
        this.eventBus.post(new InstallSnapshotRpcMessage(rpc, senderId, null));
    }

    @Override
    public void writeInstallSnapshotResult(InstallSnapshotResult result, NodeId senderId, InstallSnapshotRpc rpc) {
        this.eventBus.post(new InstallSnapshotResultMessage(result, senderId, rpc));
    }

    @Override
    public void close() {
    }

}
