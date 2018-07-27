package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.EntryApplier;
import in.xnnyygn.xraft.core.log.entry.GeneralEntry;
import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.log.entry.NoOpEntry;
import in.xnnyygn.xraft.core.log.snapshot.SnapshotApplier;
import in.xnnyygn.xraft.core.log.snapshot.SnapshotGenerator;
import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpc;

import java.util.Set;

public interface Log {

    NoOpEntry appendEntry(int term);

    GeneralEntry appendEntry(int term, byte[] command);

    GroupConfigEntry appendEntry(int term, Set<NodeConfig> nodeConfigs);

    // TODO rename me?
    GroupConfigEntry getLastGroupConfigEntry();

    int getCommitIndex();

    boolean appendEntries(AppendEntriesRpc rpc);

    @Deprecated
    RequestVoteRpc createRequestVoteRpc(int term, NodeId selfNodeId);

    void setLastEntryIndexAndTerm(RequestVoteRpc rpc);

    AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfNodeId, int nextIndex, int maxEntries);

    InstallSnapshotRpc createInstallSnapshotRpc(int term, NodeId selfNodeId, int offset);

    void advanceCommitIndex(int newCommitIndex, int currentTerm);

    int getNextLogIndex();

    boolean isNewerThan(int lastLogIndex, int lastLogTerm);

    void setEntryApplier(EntryApplier applier);

    void installSnapshot(InstallSnapshotRpc rpc);

    // TODO rename to enableSnapshot?
    void setSnapshotGenerator(SnapshotGenerator generator);

    void setSnapshotApplier(SnapshotApplier applier);

}
