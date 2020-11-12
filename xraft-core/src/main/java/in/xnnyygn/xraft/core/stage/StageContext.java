package in.xnnyygn.xraft.core.stage;

import in.xnnyygn.xraft.core.rpc.message.AbstractRpcMessage;

import java.util.concurrent.TimeUnit;

public class StageContext {
    private final StageTimer timer = new StageTimer();
    private final MessageDispatcher messageDispatcher;

    public StageContext(MessageDispatcher messageDispatcher) {
        this.messageDispatcher = messageDispatcher;
    }

    public TimeoutTimer scheduleTimeout(Runnable action, long delay, TimeUnit unit) {
        return new TimeoutTimer(timer, action, delay, unit);
    }

    public <T extends AbstractRpcMessage<?>> MessageListener<T> addMessageListener(MessageListener<T> listener) {
        messageDispatcher.addMessageListener(listener);
        return listener;
    }

    public <T extends AbstractRpcMessage<?>> void removeMessageListener(MessageListener<T> listener) {
    }

    public <T> void handOver(String stageName, T message) {
    }
}
