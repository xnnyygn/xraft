package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.*;
import in.xnnyygn.xraft.core.log.snapshot.SnapshotApplier;
import in.xnnyygn.xraft.core.log.snapshot.SnapshotGenerator;
import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpc;

import java.util.Set;

// separate log: supporting snapshot or not
public interface Log {

    // entries, read
    RequestVoteRpc createRequestVoteRpc(int term, NodeId selfNodeId);

    // snapshot, entries, read
    AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfNodeId, int nextIndex, int maxEntries);

    // snapshot, entries, write
    InstallSnapshotRpc createInstallSnapshotRpc(int term, NodeId selfNodeId, int offset);

    // get last group config entry, entries, read
    // TODO rename me?
    GroupConfigEntry getLastGroupConfigEntry();

    // state, read
    int getCommitIndex();

    // state, read
    int getNextIndex();

    // snapshot, entries read
    boolean isNewerThan(int lastLogIndex, int lastLogTerm);

    // append entry, entries, write
    NoOpEntry appendEntry(int term);

    GeneralEntry appendEntry(int term, byte[] command);

    GroupConfigEntry appendEntry(int term, Set<NodeConfig> nodeConfigs);

    // entries, write
    boolean appendEntries(AppendEntriesRpc rpc);

    // entries, write
    void advanceCommitIndex(int newCommitIndex, int currentTerm);

    // snapshot, write
    void installSnapshot(InstallSnapshotRpc rpc);

    void setEntryApplier(EntryApplier applier);

    // TODO rename to enableSnapshot?
    void setSnapshotGenerator(SnapshotGenerator generator);

    void setSnapshotApplier(SnapshotApplier applier);

}
