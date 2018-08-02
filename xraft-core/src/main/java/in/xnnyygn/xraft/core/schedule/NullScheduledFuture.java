package in.xnnyygn.xraft.core.schedule;

import javax.annotation.Nonnull;
import java.util.concurrent.*;

public class NullScheduledFuture implements ScheduledFuture<Object> {

    @Override
    public long getDelay(@Nonnull TimeUnit unit) {
        return 0;
    }

    @Override
    public int compareTo(@Nonnull Delayed o) {
        return 0;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        return null;
    }

    @Override
    public Object get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }

}
