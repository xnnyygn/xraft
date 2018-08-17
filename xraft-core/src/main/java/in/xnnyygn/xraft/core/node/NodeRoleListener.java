package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.node.role.RoleState;

import javax.annotation.Nonnull;

/**
 * Node role listener.
 */
public interface NodeRoleListener {

    /**
     * Called when node role changes. e.g FOLLOWER to CANDIDATE.
     *
     * @param roleState role state
     */
    void nodeRoleChanged(@Nonnull RoleState roleState);

}
