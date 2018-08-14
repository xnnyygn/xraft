package in.xnnyygn.xraft.core.support;

import com.google.common.util.concurrent.FutureCallback;

import java.util.Collection;
import java.util.concurrent.Future;

public interface TaskExecutor {

    Future<?> submit(Runnable task);

    void submit(Runnable task, FutureCallback<Object> callback);

    void submit(Runnable task, Collection<FutureCallback<Object>> callbacks);

    void shutdown() throws InterruptedException;

}
