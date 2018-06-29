package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpc;

public interface Log {

    /**
     * Append NO-OP entry.
     *
     * @param term term
     */
    void appendEntry(int term);

    void appendEntry(int term, byte[] command);

    void appendEntry(int term, byte[] command, EntryApplier applier);

    boolean appendEntries(AppendEntriesRpc rpc);

    RequestVoteRpc createRequestVoteRpc(int term, NodeId selfNodeId);

    AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfNodeId, int nextIndex, int maxEntries);

    void advanceCommitIndexIfAvailable(int newCommitIndex);

    int getLastLogIndex();

    boolean isNewerThan(int lastLogIndex, int lastLogTerm);

    void setEntryApplier(EntryApplier applier);

}
