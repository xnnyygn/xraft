package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;

public class NullGroupConfigChangeTask implements GroupConfigChangeTask {

    @Override
    public void onLogCommitted(GroupConfigEntry entry) {
    }

    @Override
    public GroupConfigChangeTaskResult call() throws Exception {
        return null;
    }

    @Override
    public void setGroupConfigEntry(GroupConfigEntry entry) {
    }

    @Override
    public String toString() {
        return "NullGroupConfigChangeTask{}";
    }

}
