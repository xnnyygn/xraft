package in.xnnyygn.xraft.messages;

import in.xnnyygn.xraft.server.RaftNodeId;
import in.xnnyygn.xraft.rpc.RequestVoteRpc;

public class RequestVoteRpcMessage implements RaftMessage {

    private final RequestVoteRpc rpc;

    public RequestVoteRpcMessage(RequestVoteRpc rpc) {
        this.rpc = rpc;
    }

    public RequestVoteRpc getRpc() {
        return rpc;
    }

    public RaftNodeId getSenderNodeId() {
        return this.rpc.getCandidateId();
    }

    @Override
    public String toString() {
        return "RequestVoteRpcMessage{" +
                "rpc=" + rpc +
                '}';
    }

}
