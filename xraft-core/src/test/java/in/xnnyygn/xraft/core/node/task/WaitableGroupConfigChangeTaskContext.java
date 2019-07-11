package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;

class WaitableGroupConfigChangeTaskContext implements GroupConfigChangeTaskContext {

    private boolean logAppended = false;

    @Override
    public synchronized void addNode(NodeEndpoint endpoint, int nextIndex, int matchIndex) {
        logAppended = true;
        notify();
    }

    @Override
    public void downgradeSelf() {
        logAppended = true;
        notify();
    }

    @Override
    public void removeNode(NodeId nodeId) {
    }

    @Override
    public void done() {
    }

    synchronized void awaitLogAppended() throws InterruptedException {
        if(!logAppended) {
            wait();
        }
        logAppended = true;
    }

}
