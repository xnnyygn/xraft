package in.xnnyygn.xraft.core.support;

import com.google.common.util.concurrent.FutureCallback;

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
    public Future<?> submit(Runnable task) {
        return executorService.submit(task);
    }

    @Override
    public <V> Future<V> submit(Callable<V> task) {
        return executorService.submit(task);
    }

    @Override
    public void submit(Runnable task, Collection<FutureCallback<Object>> callbacks) {
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
