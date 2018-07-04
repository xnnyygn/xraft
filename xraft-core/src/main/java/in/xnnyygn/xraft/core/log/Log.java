package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.snapshot.SnapshotApplier;
import in.xnnyygn.xraft.core.log.snapshot.SnapshotGenerator;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpc;

public interface Log {

    /**
     * Append NO-OP entry.
     *
     * @param term term
     */
    void appendEntry(int term);

    void appendEntry(int term, byte[] command);

    boolean appendEntries(AppendEntriesRpc rpc);

    RequestVoteRpc createRequestVoteRpc(int term, NodeId selfNodeId);

    AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfNodeId, int nextIndex, int maxEntries);

    InstallSnapshotRpc createInstallSnapshotRpc(int term, NodeId selfNodeId, int offset);

    void advanceCommitIndexIfAvailable(int newCommitIndex);

    int getNextLogIndex();

    boolean isNewerThan(int lastLogIndex, int lastLogTerm);

    void setEntryApplier(EntryApplier applier);

    void installSnapshot(InstallSnapshotRpc rpc);

    // TODO rename to enableSnapshot?
    void setSnapshotGenerator(SnapshotGenerator generator);

    void setSnapshotApplier(SnapshotApplier applier);

}
