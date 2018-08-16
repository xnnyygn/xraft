package in.xnnyygn.xraft.core.node;

public interface GroupConfigChangeTaskContext {

    // in node thread
    // add node to group
    // add log entry
    // replicate
    void doAddNode(NodeEndpoint endpoint, int nextIndex, int matchIndex);

    // in node thread
    void replicateLog(NodeEndpoint endpoint);

    void doReplicateLog(NodeEndpoint endpoint, int nextIndex);

    // downgrade node
    // add log entry
    // replicate
    void downgradeNode(NodeId nodeId);

    // TODO add test
    // remove node from group
    void removeNode(NodeId nodeId);

    // remove task
    void taskDone();

}
