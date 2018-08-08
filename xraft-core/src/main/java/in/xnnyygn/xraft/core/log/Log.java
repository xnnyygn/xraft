package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.*;
import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

import java.util.List;
import java.util.Set;

public interface Log {

    EntryMeta getLastEntryMeta();

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

    AddNodeEntry appendEntryForAddNode(int term, Set<NodeConfig> nodeConfigs, NodeConfig newNodeConfig);

    RemoveNodeEntry appendEntryForRemoveNode(int term, Set<NodeConfig> nodeConfigs, NodeId nodeToRemove);

    boolean appendEntries(int prevLogIndex, int prevLogTerm, List<Entry> entries);

    // entries, write
    void advanceCommitIndex(int newCommitIndex, int currentTerm);

    // snapshot, write
    void installSnapshot(InstallSnapshotRpc rpc);

    void setStateMachine(StateMachine stateMachine);

    void close();

}
