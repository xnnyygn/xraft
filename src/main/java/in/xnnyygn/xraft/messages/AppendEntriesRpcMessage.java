package in.xnnyygn.xraft.messages;

import in.xnnyygn.xraft.server.RaftNodeId;
import in.xnnyygn.xraft.rpc.AppendEntriesRpc;

public class AppendEntriesRpcMessage implements RaftMessage {

    private final AppendEntriesRpc rpc;

    public AppendEntriesRpcMessage(AppendEntriesRpc rpc) {
        this.rpc = rpc;
    }

    public AppendEntriesRpc getRpc() {
        return rpc;
    }

    public RaftNodeId getSenderNodeId() {
        return this.rpc.getLeaderId();
    }

    @Override
    public String toString() {
        return "AppendEntriesRpcMessage{" +
                "rpc=" + rpc +
                '}';
    }

}
