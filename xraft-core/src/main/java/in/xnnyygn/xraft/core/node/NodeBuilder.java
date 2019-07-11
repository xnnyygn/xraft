package in.xnnyygn.xraft.core.node;

import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.FileLog;
import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.node.config.NodeConfig;
import in.xnnyygn.xraft.core.node.store.FileNodeStore;
import in.xnnyygn.xraft.core.node.store.MemoryNodeStore;
import in.xnnyygn.xraft.core.node.store.NodeStore;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.rpc.nio.NioConnector;
import in.xnnyygn.xraft.core.schedule.DefaultScheduler;
import in.xnnyygn.xraft.core.schedule.Scheduler;
import in.xnnyygn.xraft.core.support.ListeningTaskExecutor;
import in.xnnyygn.xraft.core.support.TaskExecutor;
import io.netty.channel.nio.NioEventLoopGroup;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executors;

/**
 * Node builder.
 */
public class NodeBuilder {

    /**
     * Group.
     */
    private final NodeGroup group;

    /**
     * Self id.
     */
    private final NodeId selfId;

    /**
     * Event bus, INTERNAL.
     */
    private final EventBus eventBus;

    /**
     * Node configuration.
     */
    private NodeConfig config = new NodeConfig();

    /**
     * Starts as standby or not.
     */
    private boolean standby = false;

    /**
     * Log.
     * If data directory specified, {@link FileLog} will be created.
     * Default to {@link MemoryLog}.
     */
    private Log log = null;

    /**
     * Store for current term and last node id voted for.
     * If data directory specified, {@link FileNodeStore} will be created.
     * Default to {@link MemoryNodeStore}.
     */
    private NodeStore store = null;

    /**
     * Scheduler, INTERNAL.
     */
    private Scheduler scheduler = null;

    /**
     * Connector, component to communicate between nodes, INTERNAL.
     */
    private Connector connector = null;

    /**
     * Task executor for node, INTERNAL.
     */
    private TaskExecutor taskExecutor = null;

    /**
     * Task executor for group config change task, INTERNAL.
     */
    private TaskExecutor groupConfigChangeTaskExecutor = null;

    /**
     * Event loop group for worker.
     * If specified, reuse. otherwise create one.
     */
    private NioEventLoopGroup workerNioEventLoopGroup = null;

    // TODO add doc
    public NodeBuilder(@Nonnull NodeEndpoint endpoint) {
        this(Collections.singletonList(endpoint), endpoint.getId());
    }

    // TODO add doc
    public NodeBuilder(@Nonnull Collection<NodeEndpoint> endpoints, @Nonnull NodeId selfId) {
        Preconditions.checkNotNull(endpoints);
        Preconditions.checkNotNull(selfId);
        this.group = new NodeGroup(endpoints, selfId);
        this.selfId = selfId;
        this.eventBus = new EventBus(selfId.getValue());
    }

    /**
     * Create.
     *
     * @param selfId self id
     * @param group  group
     */
    @Deprecated
    public NodeBuilder(@Nonnull NodeId selfId, @Nonnull NodeGroup group) {
        Preconditions.checkNotNull(selfId);
        Preconditions.checkNotNull(group);
        this.selfId = selfId;
        this.group = group;
        this.eventBus = new EventBus(selfId.getValue());
    }

    /**
     * Set standby.
     *
     * @param standby standby
     * @return this
     */
    public NodeBuilder setStandby(boolean standby) {
        this.standby = standby;
        return this;
    }

    /**
     * Set configuration.
     *
     * @param config config
     * @return this
     */
    public NodeBuilder setConfig(@Nonnull NodeConfig config) {
        Preconditions.checkNotNull(config);
        this.config = config;
        return this;
    }

    /**
     * Set connector.
     *
     * @param connector connector
     * @return this
     */
    NodeBuilder setConnector(@Nonnull Connector connector) {
        Preconditions.checkNotNull(connector);
        this.connector = connector;
        return this;
    }

    /**
     * Set event loop for worker.
     * If specified, it's caller's responsibility to close worker event loop.
     *
     * @param workerNioEventLoopGroup worker event loop
     * @return this
     */
    public NodeBuilder setWorkerNioEventLoopGroup(@Nonnull NioEventLoopGroup workerNioEventLoopGroup) {
        Preconditions.checkNotNull(workerNioEventLoopGroup);
        this.workerNioEventLoopGroup = workerNioEventLoopGroup;
        return this;
    }

    /**
     * Set scheduler.
     *
     * @param scheduler scheduler
     * @return this
     */
    NodeBuilder setScheduler(@Nonnull Scheduler scheduler) {
        Preconditions.checkNotNull(scheduler);
        this.scheduler = scheduler;
        return this;
    }

    /**
     * Set task executor.
     *
     * @param taskExecutor task executor
     * @return this
     */
    NodeBuilder setTaskExecutor(@Nonnull TaskExecutor taskExecutor) {
        Preconditions.checkNotNull(taskExecutor);
        this.taskExecutor = taskExecutor;
        return this;
    }

    /**
     * Set group config change task executor.
     *
     * @param groupConfigChangeTaskExecutor group config change task executor
     * @return this
     */
    NodeBuilder setGroupConfigChangeTaskExecutor(@Nonnull TaskExecutor groupConfigChangeTaskExecutor) {
        Preconditions.checkNotNull(groupConfigChangeTaskExecutor);
        this.groupConfigChangeTaskExecutor = groupConfigChangeTaskExecutor;
        return this;
    }

    /**
     * Set store.
     *
     * @param store store
     * @return this
     */
    NodeBuilder setStore(@Nonnull NodeStore store) {
        Preconditions.checkNotNull(store);
        this.store = store;
        return this;
    }

    /**
     * Set data directory.
     *
     * @param dataDirPath data directory
     * @return this
     */
    public NodeBuilder setDataDir(@Nullable String dataDirPath) {
        if (dataDirPath == null || dataDirPath.isEmpty()) {
            return this;
        }
        File dataDir = new File(dataDirPath);
        if (!dataDir.isDirectory() || !dataDir.exists()) {
            throw new IllegalArgumentException("[" + dataDirPath + "] not a directory, or not exists");
        }
        log = new FileLog(dataDir, eventBus, group.listEndpointOfMajor());
        store = new FileNodeStore(new File(dataDir, FileNodeStore.FILE_NAME));
        return this;
    }

    /**
     * Build node.
     *
     * @return node
     */
    @Nonnull
    public Node build() {
        return new NodeImpl(buildContext());
    }

    /**
     * Build context for node.
     *
     * @return node context
     */
    @Nonnull
    private NodeContext buildContext() {
        NodeContext context = new NodeContext();
        context.setGroup(group);
        context.setMode(evaluateMode());
        context.setLog(log != null ? log : new MemoryLog(eventBus, group.listEndpointOfMajor()));
        context.setStore(store != null ? store : new MemoryNodeStore());
        context.setSelfId(selfId);
        context.setConfig(config);
        context.setEventBus(eventBus);
        context.setScheduler(scheduler != null ? scheduler : new DefaultScheduler(config));
        context.setConnector(connector != null ? connector : createNioConnector());
        context.setTaskExecutor(taskExecutor != null ? taskExecutor : new ListeningTaskExecutor(
                Executors.newSingleThreadExecutor(r -> new Thread(r, "node"))
        ));
        // TODO share monitor
        context.setGroupConfigChangeTaskExecutor(groupConfigChangeTaskExecutor != null ? groupConfigChangeTaskExecutor :
                new ListeningTaskExecutor(Executors.newSingleThreadExecutor(r -> new Thread(r, "group-config-change"))));
        return context;
    }

    /**
     * Create nio connector.
     *
     * @return nio connector
     */
    @Nonnull
    private NioConnector createNioConnector() {
        int port = group.findSelf().getEndpoint().getPort();
        if (workerNioEventLoopGroup != null) {
            return new NioConnector(workerNioEventLoopGroup, selfId, eventBus, port);
        }
        return new NioConnector(new NioEventLoopGroup(config.getNioWorkerThreads()), false, selfId, eventBus, port);
    }

    /**
     * Evaluate mode.
     *
     * @return mode
     * @see NodeGroup#isStandalone()
     */
    @Nonnull
    private NodeMode evaluateMode() {
        if (standby) {
            return NodeMode.STANDBY;
        }
        if (group.isStandalone()) {
            return NodeMode.STANDALONE;
        }
        return NodeMode.GROUP_MEMBER;
    }

}
