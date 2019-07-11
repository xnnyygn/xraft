package in.xnnyygn.xraft.core.node.task;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeoutException;

/**
 * Reference for group config change task.
 */
public interface GroupConfigChangeTaskReference {

    /**
     * Wait for result forever.
     *
     * @return result
     * @throws InterruptedException if interrupted
     */
    @Nonnull
    GroupConfigChangeTaskResult getResult() throws InterruptedException;

    /**
     * Wait for result in specified timeout.
     *
     * @param timeout timeout
     * @return result
     * @throws InterruptedException if interrupted
     * @throws TimeoutException if timeout
     */
    @Nonnull
    GroupConfigChangeTaskResult getResult(long timeout) throws InterruptedException, TimeoutException;

    default void awaitDone(long timeout) throws TimeoutException, InterruptedException {
        if (timeout == 0) {
            getResult();
        } else {
            getResult(timeout);
        }
    }

    /**
     * Cancel task.
     */
    void cancel();

}
