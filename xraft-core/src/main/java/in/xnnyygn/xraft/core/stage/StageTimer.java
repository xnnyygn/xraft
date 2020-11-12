package in.xnnyygn.xraft.core.stage;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class StageTimer {
    private final ScheduledExecutorService scheduledExecutorService
            = Executors.newSingleThreadScheduledExecutor((r) -> new Thread(r, "stage-timer"));

    public ScheduledFuture<?> schedule(Runnable action, long delay, TimeUnit unit) {
        return scheduledExecutorService.schedule(action, delay, unit);
    }

    // TODO shutdown
    public void shutdown() {

    }
}
