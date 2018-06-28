package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.rpc.ChannelException;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.schedule.Scheduler;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class NodeContext {

    private static final Logger logger = LoggerFactory.getLogger(NodeContext.class);
    private final NodeId selfNodeId;
    private final NodeGroup nodeGroup;
    private final NodeStore nodeStore;
    private final ExecutorService monitorExecutorService;

    private EventBus eventBus;
    private Log log;
    private Scheduler scheduler;
    private Connector connector;

    public NodeContext(NodeId selfNodeId, NodeGroup nodeGroup, NodeStore nodeStore) {
        this.selfNodeId = selfNodeId;
        this.nodeGroup = nodeGroup;
        this.nodeStore = nodeStore;
        this.monitorExecutorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "monitor-" + selfNodeId));
    }

    public void initialize() {
        this.connector.initialize();
    }

    public NodeId getSelfNodeId() {
        return this.selfNodeId;
    }

    public NodeGroup getNodeGroup() {
        return nodeGroup;
    }

    public NodeStore getNodeStore() {
        return nodeStore;
    }

    public Log getLog() {
        return log;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public Connector getConnector() {
        return connector;
    }

    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
        this.eventBus.register(this);
    }

    public EventBus getEventBus() {
        return eventBus;
    }

    public void setLog(Log log) {
        this.log = log;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public void register(Object eventSubscriber) {
        this.eventBus.register(eventSubscriber);
    }

    public void runWithMonitor(ListeningExecutorService executorService, Runnable r) {
        ListenableFuture<?> future = executorService.submit(r);
        Futures.addCallback(future, new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object result) {
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof ChannelException) {
                    logger.warn(t.getMessage());
                } else {
                    logger.warn("failure", t);
                }
            }
        }, this.monitorExecutorService);
    }

    @Subscribe
    public void onReceive(DeadEvent deadEvent) {
        logger.warn("dead event {}", deadEvent);
    }

    public void release() throws InterruptedException {
        this.scheduler.stop();
        this.connector.release();
        this.monitorExecutorService.shutdown();
        this.monitorExecutorService.awaitTermination(1L, TimeUnit.SECONDS);
    }

}
