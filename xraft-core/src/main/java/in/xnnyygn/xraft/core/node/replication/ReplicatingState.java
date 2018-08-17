package in.xnnyygn.xraft.core.node.replication;

import in.xnnyygn.xraft.core.node.NodeId;

import javax.annotation.Nonnull;

/**
 * Replicating state.
 */
public interface ReplicatingState {

    /**
     * Get node id.
     *
     * @return node id
     */
    @Nonnull
    NodeId getNodeId();

    /**
     * Get next index.
     *
     * @return next index
     */
    int getNextIndex();

    /**
     * Get match index.
     *
     * @return match index
     */
    int getMatchIndex();

    /**
     * Back off next index, in other word, decrease.
     *
     * @return true if decrease successfully, false if next index is less than or equal to {@code 1}
     */
    boolean backOffNextIndex();

    /**
     * Advance next index and match index by last entry index.
     *
     * @param lastEntryIndex last entry index
     * @return true if advanced, false if no change
     */
    boolean advance(int lastEntryIndex);

    /**
     * Test if current node is replication target.
     * <p>
     * SELF is NOT replication target.
     * </p>
     *
     * @return true if is, otherwise false
     */
    boolean isTarget();

    /**
     * Test if replicating.
     *
     * @return true if replicating, otherwise false
     */
    boolean isReplicating();

    /**
     * Set replicating.
     *
     * @param replicating replicating
     */
    void setReplicating(boolean replicating);

    /**
     * Get last replicated timestamp.
     *
     * @return last replicated timestamp
     */
    long getLastReplicatedAt();

    /**
     * Set last replicated timestamp.
     *
     * @param lastReplicatedAt last replicated timestamp
     */
    void setLastReplicatedAt(long lastReplicatedAt);

}
