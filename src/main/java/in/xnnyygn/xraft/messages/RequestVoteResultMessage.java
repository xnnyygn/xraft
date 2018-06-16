package in.xnnyygn.xraft.messages;

import in.xnnyygn.xraft.rpc.RequestVoteResult;

public class RequestVoteResultMessage extends AbstractResultMessage<RequestVoteResult> {

    public RequestVoteResultMessage(RequestVoteResult result) {
        super(result);
    }

}
