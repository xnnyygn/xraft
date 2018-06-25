package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;

public interface Log {

    void appendEntry(int term, byte[] command);

    void appendEntry(int term, byte[] command, EntryApplier applier);

    boolean appendEntries(AppendEntriesRpc rpc);

    AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfNodeId, int nextIndex, int maxEntries);

    void advanceCommitIndexIfAvailable(int newCommitIndex);

    int getLastLogIndex();

    boolean isNewerThan(int lastLogIndex, int lastLogTerm);

    void setEntryApplier(EntryApplier callback);

}
