package in.xnnyygn.xraft.messages;

import in.xnnyygn.xraft.rpc.AppendEntriesResult;

public class AppendEntriesResultMessage extends AbstractResultMessage<AppendEntriesResult> {

    public AppendEntriesResultMessage(AppendEntriesResult result) {
        super(result);
    }

}
