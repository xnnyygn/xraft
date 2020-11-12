package in.xnnyygn.xraft.core.stage;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TimeoutTimer {
    private final StageTimer stageTimer;
    private final Runnable action;
    private final long delay;
    private final TimeUnit unit;
    private ScheduledFuture<?> scheduledFuture;

    public TimeoutTimer(StageTimer stageTimer, Runnable action, long delay, TimeUnit unit) {
        this.stageTimer = stageTimer;
        this.action = action;
        this.delay = delay;
        this.unit = unit;
    }

    public void start() {
        schedule();
    }

    public void reset() {
        cancel();
        schedule();
    }

    public void cancel() {
        if (scheduledFuture == null) {
            throw new IllegalStateException("no schedule future");
        }
        scheduledFuture.cancel(false);
    }

    private void schedule() {
        scheduledFuture = stageTimer.schedule(action, delay, unit);
    }
}
