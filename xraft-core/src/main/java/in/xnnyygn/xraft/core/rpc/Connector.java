package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.*;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;

/**
 * Connector.
 */
public interface Connector {

    /**
     * Initialize connector.
     * <p>
     * SHOULD NOT call more than one.
     * </p>
     */
    void initialize();

    void sendPreVote(@Nonnull PreVoteRpc rpc, @Nonnull Collection<NodeEndpoint> destinationEndpoints);

    void replyPreVote(@Nonnull PreVoteResult result, @Nonnull PreVoteRpcMessage rpcMessage);

    /**
     * Send request vote rpc.
     * <p>
     * Remember to exclude self node before sending.
     * </p>
     * <p>
     * Do nothing if destination endpoints is empty.
     * </p>
     *
     * @param rpc                  rpc
     * @param destinationEndpoints destination endpoints
     */
    void sendRequestVote(@Nonnull RequestVoteRpc rpc, @Nonnull Collection<NodeEndpoint> destinationEndpoints);

    /**
     * Reply request vote result.
     *
     * @param result     result
     * @param rpcMessage rpc message
     */
    void replyRequestVote(@Nonnull RequestVoteResult result, @Nonnull RequestVoteRpcMessage rpcMessage);

    /**
     * Send append entries rpc.
     *
     * @param rpc                 rpc
     * @param destinationEndpoint destination endpoint
     */
    void sendAppendEntries(@Nonnull AppendEntriesRpc rpc, @Nonnull NodeEndpoint destinationEndpoint);

    /**
     * Reply append entries result.
     *
     * @param result result
     * @param rpcMessage rpc message
     */
    void replyAppendEntries(@Nonnull AppendEntriesResult result, @Nonnull AppendEntriesRpcMessage rpcMessage);

    /**
     * Send install snapshot rpc.
     *
     * @param rpc rpc
     * @param destinationEndpoint destination endpoint
     */
    void sendInstallSnapshot(@Nonnull InstallSnapshotRpc rpc, @Nonnull NodeEndpoint destinationEndpoint);

    /**
     * Reply install snapshot result.
     *
     * @param result result
     * @param rpcMessage rpc message
     */
    void replyInstallSnapshot(@Nonnull InstallSnapshotResult result, @Nonnull InstallSnapshotRpcMessage rpcMessage);

    /**
     * Called when node becomes leader.
     * <p>
     * Connector may use this chance to close inbound channels.
     * </p>
     */
    void resetChannels();

    /**
     * Close connector.
     */
    void close();

}
