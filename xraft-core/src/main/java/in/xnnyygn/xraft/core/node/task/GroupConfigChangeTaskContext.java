package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;

public interface GroupConfigChangeTaskContext {

    // in node thread
    // add node to group
    // add log entry
    // replicate
    void doAddNode(NodeEndpoint endpoint, int nextIndex, int matchIndex);

    // downgrade node
    // add log entry
    // replicate
    void downgradeNode(NodeId nodeId);

    // TODO add test
    // remove node from group
    void removeNode(NodeId nodeId);

    // remove task
    void done();

}
