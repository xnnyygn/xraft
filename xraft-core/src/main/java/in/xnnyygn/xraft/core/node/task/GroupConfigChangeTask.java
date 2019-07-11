package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.node.NodeId;

import java.util.concurrent.Callable;

public interface GroupConfigChangeTask extends Callable<GroupConfigChangeTaskResult> {

    GroupConfigChangeTask NONE = new NullGroupConfigChangeTask();

    void setGroupConfigEntry(GroupConfigEntry entry);

    void onLogCommitted(GroupConfigEntry entry);

}
