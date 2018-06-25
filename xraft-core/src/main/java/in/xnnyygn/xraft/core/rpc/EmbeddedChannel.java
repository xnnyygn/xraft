package in.xnnyygn.xraft.core.rpc;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeId;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EmbeddedChannel implements Channel {

    private final ExecutorService executorService;
    private final EventBus eventBus;

    public EmbeddedChannel(NodeId selfNodeId, EventBus eventBus) {
        this.executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "embedded-channel-" + selfNodeId));
        this.eventBus = eventBus;
    }

    @Override
    public void send(Object payload, NodeId senderId) {
        this.executorService.submit(() -> this.eventBus.post(payload));
    }

    @Override
    public void close() {
        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new ChannelException("failed to stop channel", e);
        }
    }

}
