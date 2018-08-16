package in.xnnyygn.xraft.core.node.task;

import java.util.concurrent.TimeoutException;

public interface GroupConfigChangeTaskReference {

    GroupConfigChangeTaskResult getResult() throws InterruptedException;

    GroupConfigChangeTaskResult getResult(long timeout) throws InterruptedException, TimeoutException;

    void cancel();

}
