package in.xnnyygn.xraft.core.support;

import com.google.common.util.concurrent.*;

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
    public Future<?> submit(Runnable task) {
        return listeningExecutorService.submit(task);
    }

    @Override
    public <V> Future<V> submit(Callable<V> task) {
        return listeningExecutorService.submit(task);
    }

    @Override
    public void submit(Runnable task, Collection<FutureCallback<Object>> callbacks) {
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
