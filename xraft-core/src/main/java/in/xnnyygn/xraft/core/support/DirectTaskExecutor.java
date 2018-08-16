package in.xnnyygn.xraft.core.support;

import com.google.common.util.concurrent.FutureCallback;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class DirectTaskExecutor extends AbstractTaskExecutor {

    private final boolean throwWhenFailed;

    public DirectTaskExecutor() {
        this(false);
    }

    public DirectTaskExecutor(boolean throwWhenFailed) {
        this.throwWhenFailed = throwWhenFailed;
    }

    @Override
    public Future<?> submit(Runnable task) {
        FutureTask<?> futureTask = new FutureTask<>(task, null);
        futureTask.run();
        return futureTask;
    }

    @Override
    public <V> Future<V> submit(Callable<V> task) {
        FutureTask<V> futureTask = new FutureTask<V>(task);
        futureTask.run();
        return futureTask;
    }

    @Override
    public void submit(Runnable task, Collection<FutureCallback<Object>> callbacks) {
        try {
            task.run();
            callbacks.forEach(c -> c.onSuccess(null));
        } catch (Throwable t) {
            callbacks.forEach(c -> c.onFailure(t));
            if (throwWhenFailed) {
                throw t;
            }
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
    }

}
