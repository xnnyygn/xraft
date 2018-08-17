package in.xnnyygn.xraft.core.support;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.concurrent.*;

public class SingleThreadTaskExecutor extends AbstractTaskExecutor {

    private final ExecutorService executorService;

    public SingleThreadTaskExecutor() {
        this(Executors.defaultThreadFactory());
    }

    public SingleThreadTaskExecutor(String name) {
        this(r -> new Thread(r, name));
    }

    private SingleThreadTaskExecutor(ThreadFactory threadFactory) {
        executorService = Executors.newSingleThreadExecutor(threadFactory);
    }

    @Override
    @Nonnull
    public Future<?> submit(@Nonnull Runnable task) {
        Preconditions.checkNotNull(task);
        return executorService.submit(task);
    }

    @Override
    @Nonnull
    public <V> Future<V> submit(@Nonnull Callable<V> task) {
        Preconditions.checkNotNull(task);
        return executorService.submit(task);
    }

    @Override
    public void submit(@Nonnull Runnable task, @Nonnull Collection<FutureCallback<Object>> callbacks) {
        Preconditions.checkNotNull(task);
        Preconditions.checkNotNull(callbacks);
        executorService.submit(() -> {
            try {
                task.run();
                callbacks.forEach(c -> c.onSuccess(null));
            } catch (Exception e) {
                callbacks.forEach(c -> c.onFailure(e));
            }
        });
    }

    @Override
    public void shutdown() throws InterruptedException {
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

}
