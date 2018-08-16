package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.NodeEndpoint;

public interface NewNodeCatchUpTaskContext {

    // in node thread
    void replicateLog(NodeEndpoint endpoint);

    void doReplicateLog(NodeEndpoint endpoint, int nextIndex);

    void done(NewNodeCatchUpTask task);

}
