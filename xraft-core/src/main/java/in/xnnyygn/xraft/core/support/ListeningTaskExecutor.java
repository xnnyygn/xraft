package in.xnnyygn.xraft.core.support;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.*;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.concurrent.*;

public class ListeningTaskExecutor extends AbstractTaskExecutor {

    private final ListeningExecutorService listeningExecutorService;
    private final ExecutorService monitorExecutorService;
    private final boolean monitorShared;

    public ListeningTaskExecutor(ExecutorService executorService) {
        this(MoreExecutors.listeningDecorator(executorService));
    }

    public ListeningTaskExecutor(ListeningExecutorService listeningExecutorService) {
        this(listeningExecutorService, Executors.newSingleThreadExecutor(r -> new Thread(r, "monitor")), false);
    }

    public ListeningTaskExecutor(ExecutorService executorService, ExecutorService monitorExecutorService) {
        this(MoreExecutors.listeningDecorator(executorService), monitorExecutorService, true);
    }

    private ListeningTaskExecutor(ListeningExecutorService listeningExecutorService, ExecutorService monitorExecutorService, boolean monitorShared) {
        this.listeningExecutorService = listeningExecutorService;
        this.monitorExecutorService = monitorExecutorService;
        this.monitorShared = monitorShared;
    }

    @Override
    @Nonnull
    public Future<?> submit(@Nonnull Runnable task) {
        Preconditions.checkNotNull(task);
        return listeningExecutorService.submit(task);
    }

    @Override
    @Nonnull
    public <V> Future<V> submit(@Nonnull Callable<V> task) {
        Preconditions.checkNotNull(task);
        return listeningExecutorService.submit(task);
    }

    @Override
    public void submit(@Nonnull Runnable task, @Nonnull Collection<FutureCallback<Object>> callbacks) {
        Preconditions.checkNotNull(task);
        Preconditions.checkNotNull(callbacks);
        ListenableFuture<?> future = listeningExecutorService.submit(task);
        callbacks.forEach(c -> Futures.addCallback(future, c, monitorExecutorService));
    }

    @Override
    public void shutdown() throws InterruptedException {
        listeningExecutorService.shutdown();
        listeningExecutorService.awaitTermination(1L, TimeUnit.SECONDS);
        if (!monitorShared) {
            monitorExecutorService.shutdown();
            monitorExecutorService.awaitTermination(1L, TimeUnit.SECONDS);
        }
    }

}
