package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.rpc.message.*;

import javax.annotation.Nonnull;

/**
 * Channel between nodes.
 */
public interface Channel {

    void writePreVoteRpc(@Nonnull PreVoteRpc rpc);

    void writePreVoteResult(@Nonnull PreVoteResult result);

    /**
     * Write request vote rpc.
     *
     * @param rpc rpc
     */
    void writeRequestVoteRpc(@Nonnull RequestVoteRpc rpc);

    /**
     * Write request vote result.
     *
     * @param result result
     */
    void writeRequestVoteResult(@Nonnull RequestVoteResult result);

    /**
     * Write append entries rpc.
     *
     * @param rpc rpc
     */
    void writeAppendEntriesRpc(@Nonnull AppendEntriesRpc rpc);

    /**
     * Write append entries result.
     *
     * @param result result
     */
    void writeAppendEntriesResult(@Nonnull AppendEntriesResult result);

    /**
     * Write install snapshot rpc.
     *
     * @param rpc rpc
     */
    void writeInstallSnapshotRpc(@Nonnull InstallSnapshotRpc rpc);

    /**
     * Write install snapshot result.
     *
     * @param result result
     */
    void writeInstallSnapshotResult(@Nonnull InstallSnapshotResult result);

    /**
     * Close channel.
     */
    void close();

}
