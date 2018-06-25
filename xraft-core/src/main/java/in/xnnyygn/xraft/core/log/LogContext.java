package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;

// TODO remove me
public interface LogContext {

    void sendAppendEntriesRpc(AppendEntriesRpc rpc);

}
