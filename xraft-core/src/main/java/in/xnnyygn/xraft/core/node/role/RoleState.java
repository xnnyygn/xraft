package in.xnnyygn.xraft.core.node.role;

import in.xnnyygn.xraft.core.node.NodeId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Role state.
 */
public interface RoleState {

    int VOTES_COUNT_NOT_SET = -1;

    /**
     * Get role name.
     *
     * @return role name
     */
    @Nonnull
    RoleName getRoleName();

    /**
     * Get term.
     *
     * @return term
     */
    int getTerm();

    /**
     * Get votes count.
     *
     * @return votes count, {@value VOTES_COUNT_NOT_SET} if unknown
     */
    int getVotesCount();

    /**
     * Get voted for.
     *
     * @return voted for
     */
    @Nullable
    NodeId getVotedFor();

    /**
     * Get leader id.
     *
     * @return leader id
     */
    @Nullable
    NodeId getLeaderId();

}
