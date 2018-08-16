package in.xnnyygn.xraft.core.node;

import java.util.concurrent.TimeoutException;

public interface GroupConfigChangeTaskReference {

    GroupConfigChangeTaskResult getResult(long timeout) throws InterruptedException, TimeoutException;

    void cancel();

}
