package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.task.GroupConfigChangeTaskReference;
import in.xnnyygn.xraft.core.node.task.GroupConfigChangeTaskResult;

public class FixedResultGroupConfigTaskReference implements GroupConfigChangeTaskReference {

    private final GroupConfigChangeTaskResult result;

    public FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult result) {
        this.result = result;
    }

    @Override
    public GroupConfigChangeTaskResult getResult() throws InterruptedException {
        return result;
    }

    @Override
    public GroupConfigChangeTaskResult getResult(long timeout) {
        return result;
    }

    @Override
    public void cancel() {
    }

}
