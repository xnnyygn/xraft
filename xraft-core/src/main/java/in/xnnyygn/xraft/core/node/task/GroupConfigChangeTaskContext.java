package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;

/**
 * Task context for {@link GroupConfigChangeTask}.
 */
public interface GroupConfigChangeTaskContext {

    /**
     * Add node.
     * <p>
     * Process will be run in node task executor.
     * </p>
     * <ul>
     * <li>add node to group</li>
     * <li>append log entry</li>
     * <li>replicate</li>
     * </ul>
     *
     * @param endpoint   endpoint
     * @param nextIndex  next index
     * @param matchIndex match index
     */
    void addNode(NodeEndpoint endpoint, int nextIndex, int matchIndex);

//    /**
//     * Downgrade node.
//     * <p>
//     * Process will be run in node task executor.
//     * </p>
//     * <ul>
//     * <li>downgrade node</li>
//     * <li>append log entry</li>
//     * <li>replicate</li>
//     * </ul>
//     *
//     * @param nodeId node id to downgrade
//     */
//    void downgradeNode(NodeId nodeId);

    void downgradeSelf();

    /**
     * Remove node from group.
     * <p>
     * Process will be run in node task executor.
     * </p>
     * <p>
     * if node id is self id, step down.
     * </p>
     *
     * @param nodeId node id
     */
    void removeNode(NodeId nodeId);

    /**
     * Done and remove current group config change task.
     */
    void done();

}
