package in.xnnyygn.xraft.core.node;

public class NullGroupConfigChangeTask implements GroupConfigChangeTask {

    @Override
    public boolean isTargetNode(NodeId nodeId) {
        return false;
    }

    @Override
    public void onLogCommitted() {
    }

    @Override
    public GroupConfigChangeTaskResult call() throws Exception {
        return null;
    }

    @Override
    public long getStartTime() {
        return 0;
    }

    @Override
    public String toString() {
        return "NullGroupConfigChangeTask{}";
    }

}
