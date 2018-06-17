package in.xnnyygn.xraft.messages;

import in.xnnyygn.xraft.server.ServerId;
import in.xnnyygn.xraft.rpc.RequestVoteRpc;

public class RequestVoteRpcMessage implements RaftMessage {

    private final RequestVoteRpc rpc;

    public RequestVoteRpcMessage(RequestVoteRpc rpc) {
        this.rpc = rpc;
    }

    public RequestVoteRpc getRpc() {
        return rpc;
    }

    public ServerId getSenderServerId() {
        return this.rpc.getCandidateId();
    }

    @Override
    public String toString() {
        return "RequestVoteRpcMessage{" +
                "rpc=" + rpc +
                '}';
    }

}
