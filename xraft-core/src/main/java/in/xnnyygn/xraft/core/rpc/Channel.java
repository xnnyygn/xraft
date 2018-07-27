package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.rpc.message.*;

/**
 * Channel should not be directly called.
 */
public interface Channel {

    void writeRequestVoteRpc(RequestVoteRpc rpc);

    void writeRequestVoteResult(RequestVoteResult result);

    void writeAppendEntriesRpc(AppendEntriesRpc rpc);

    void writeAppendEntriesResult(AppendEntriesResult result);

    void writeInstallSnapshotRpc(InstallSnapshotRpc rpc);

    void writeInstallSnapshotResult(InstallSnapshotResult result);

    void close();

}
