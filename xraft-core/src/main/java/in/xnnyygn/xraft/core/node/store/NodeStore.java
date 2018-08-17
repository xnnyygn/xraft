package in.xnnyygn.xraft.core.node.store;

import in.xnnyygn.xraft.core.node.NodeId;

import javax.annotation.Nullable;

/**
 * Node store.
 */
public interface NodeStore {

    /**
     * Get term.
     *
     * @return term
     */
    int getTerm();

    /**
     * Set term.
     *
     * @param term term
     */
    void setTerm(int term);

    /**
     * Get voted for.
     *
     * @return voted for
     */
    @Nullable
    NodeId getVotedFor();

    /**
     * Set voted for
     *
     * @param votedFor voted for
     */
    void setVotedFor(@Nullable NodeId votedFor);

    /**
     * Close store.
     */
    void close();

}
