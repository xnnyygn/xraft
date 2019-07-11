package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.statemachine.StateMachine;
import in.xnnyygn.xraft.core.node.role.RoleNameAndLeaderId;
import in.xnnyygn.xraft.core.node.task.GroupConfigChangeTaskReference;

import javax.annotation.Nonnull;

/**
 * Node.
 */
public interface Node {

    /**
     * Register state machine to node.
     * <p>State machine should be registered before node start, or it may not take effect.</p>
     *
     * @param stateMachine state machine
     */
    void registerStateMachine(@Nonnull StateMachine stateMachine);

    /**
     * Get current role name and leader id.
     * <p>
     * Available results:
     * </p>
     * <ul>
     * <li>FOLLOWER, current leader id</li>
     * <li>CANDIDATE, <code>null</code></li>
     * <li>LEADER, self id</li>
     * </ul>
     *
     * @return role name and leader id
     */
    @Nonnull
    RoleNameAndLeaderId getRoleNameAndLeaderId();

    /**
     * Add node role listener.
     *
     * @param listener listener
     */
    void addNodeRoleListener(@Nonnull NodeRoleListener listener);

    /**
     * Start node.
     */
    void start();

    /**
     * Append log.
     *
     * @param commandBytes command bytes
     * @throws NotLeaderException if not leader
     */
    void appendLog(@Nonnull byte[] commandBytes);

    void enqueueReadIndex(@Nonnull String requestId);

    /**
     * Add node.
     *
     * @param endpoint new node endpoint
     * @return task reference
     * @throws NotLeaderException if not leader
     * @throws IllegalStateException if group config change concurrently
     */
    @Nonnull
    GroupConfigChangeTaskReference addNode(@Nonnull NodeEndpoint endpoint);

    /**
     * Remove node.
     *
     * @param id id
     * @return task reference
     * @throws NotLeaderException if not leader
     * @throws IllegalStateException if group config change concurrently
     */
    @Nonnull
    GroupConfigChangeTaskReference removeNode(@Nonnull NodeId id);

    /**
     * Stop node.
     *
     * @throws InterruptedException if interrupted
     */
    void stop() throws InterruptedException;

}
