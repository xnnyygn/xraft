package in.xnnyygn.xraft.core.support;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nonnull;
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
    @Nonnull
    public Future<?> submit(@Nonnull Runnable task) {
        Preconditions.checkNotNull(task);
        FutureTask<?> futureTask = new FutureTask<>(task, null);
        futureTask.run();
        return futureTask;
    }

    @Override
    @Nonnull
    public <V> Future<V> submit(@Nonnull Callable<V> task) {
        Preconditions.checkNotNull(task);
        FutureTask<V> futureTask = new FutureTask<V>(task);
        futureTask.run();
        return futureTask;
    }

    @Override
    public void submit(@Nonnull Runnable task, @Nonnull Collection<FutureCallback<Object>> callbacks) {
        Preconditions.checkNotNull(task);
        Preconditions.checkNotNull(callbacks);
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
