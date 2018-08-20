package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.NodeEndpoint;

public class WaitableNewNodeCatchUpTaskContext implements NewNodeCatchUpTaskContext {

    private boolean replicated = false;

    @Override
    public synchronized void replicateLog(NodeEndpoint endpoint) {
        replicated = true;
        notify();
    }

    @Override
    public synchronized void doReplicateLog(NodeEndpoint endpoint, int nextIndex) {
        replicated = true;
        notify();
    }

    @Override
    public synchronized void sendInstallSnapshot(NodeEndpoint endpoint, int offset) {
        replicated = true;
        notify();
    }

    @Override
    public void done(NewNodeCatchUpTask task) {

    }

    synchronized void awaitReplicateLog() throws InterruptedException {
        if (!replicated) {
            wait();
        }
        replicated = false;
    }

}
