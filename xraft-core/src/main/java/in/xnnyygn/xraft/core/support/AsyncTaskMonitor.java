package in.xnnyygn.xraft.core.support;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import in.xnnyygn.xraft.core.rpc.ChannelException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.*;

public class AsyncTaskMonitor {

    private static final Logger logger = LoggerFactory.getLogger(AsyncTaskMonitor.class);
    private final ExecutorService monitorExecutorService;

    public AsyncTaskMonitor() {
        this("monitor");
    }

    public AsyncTaskMonitor(String name) {
        monitorExecutorService = Executors.newSingleThreadExecutor(r -> new Thread(r, name));
    }

    public Future<?> submit(ListeningExecutorService executorService, Runnable r) {
        ListenableFuture<?> future = executorService.submit(r);
        monitor(future);
        return future;
    }

    public <V> Future<V> submit(ListeningExecutorService executorService, Callable<V> c) {
        ListenableFuture<V> future = executorService.submit(c);
        monitor(future);
        return future;
    }

    private <V> void monitor(ListenableFuture<V> future) {
        Futures.addCallback(future, new FutureCallback<V>() {
            @Override
            public void onSuccess(@Nullable V result) {
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                if (t instanceof ChannelException) {
                    logger.warn(t.getMessage());
                } else {
                    logger.warn("failure", t);
                }
            }
        }, monitorExecutorService);
    }

    public void shutdown() throws InterruptedException {
        monitorExecutorService.shutdown();
        monitorExecutorService.awaitTermination(1L, TimeUnit.SECONDS);
    }

}
