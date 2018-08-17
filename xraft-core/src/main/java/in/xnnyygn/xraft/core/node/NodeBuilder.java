package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.FileLog;
import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.MemoryLog;
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

import java.io.File;
import java.util.concurrent.Executors;

public class NodeBuilder {

    private final NodeId selfId;
    private final NodeGroup group;
    private final EventBus eventBus;
    private NodeConfig config = new NodeConfig();
    private boolean standby = false;
    private Log log = null;
    private NodeStore store = null;
    private Scheduler scheduler = null;
    private Connector connector = null;
    private TaskExecutor taskExecutor = null;
    private TaskExecutor groupConfigChangeTaskExecutor = null;
    private NioEventLoopGroup workerNioEventLoopGroup = null;

    public NodeBuilder(NodeId selfId, NodeGroup group) {
        this.selfId = selfId;
        this.group = group;
        this.eventBus = new EventBus(selfId.getValue());
    }

    public NodeBuilder setStandby(boolean standby) {
        this.standby = standby;
        return this;
    }

    public NodeBuilder setConfig(NodeConfig config) {
        this.config = config;
        return this;
    }

    public NodeBuilder setConnector(Connector connector) {
        this.connector = connector;
        return this;
    }

    public NodeBuilder setWorkerNioEventLoopGroup(NioEventLoopGroup workerNioEventLoopGroup) {
        this.workerNioEventLoopGroup = workerNioEventLoopGroup;
        return this;
    }

    public NodeBuilder setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    public NodeBuilder setTaskExecutor(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
        return this;
    }

    public NodeBuilder setGroupConfigChangeTaskExecutor(TaskExecutor groupConfigChangeTaskExecutor) {
        this.groupConfigChangeTaskExecutor = groupConfigChangeTaskExecutor;
        return this;
    }

    public NodeBuilder setStore(NodeStore store) {
        this.store = store;
        return this;
    }

    public NodeBuilder setDataDir(String dataDirPath) {
        if (dataDirPath == null || dataDirPath.isEmpty()) {
            return this;
        }
        File dataDir = new File(dataDirPath);
        if (!dataDir.isDirectory() || !dataDir.exists()) {
            throw new IllegalArgumentException("[" + dataDirPath + "] not a directory, or not exists");
        }
        log = new FileLog(dataDir, eventBus);
        store = new FileNodeStore(new File(dataDir, FileNodeStore.FILE_NAME));
        return this;
    }

    public Node build() {
        return new NodeImpl(buildNodeContext());
    }

    private NodeContext buildNodeContext() {
        NodeContext context = new NodeContext();
        context.setGroup(group);
        context.setMode(evaluateMode());
        context.setLog(log != null ? log : new MemoryLog(eventBus));
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

    private NioConnector createNioConnector() {
        int port = group.findEndpoint(selfId).getPort();
        if (workerNioEventLoopGroup != null) {
            return new NioConnector(workerNioEventLoopGroup, selfId, eventBus, port);
        }
        return new NioConnector(new NioEventLoopGroup(config.getNioWorkerThreads()), selfId, eventBus, port);
    }

    private NodeMode evaluateMode() {
        if (standby) {
            return NodeMode.STANDBY;
        }
        if (group.isUniqueNode(selfId)) {
            return NodeMode.STANDALONE;
        }
        return NodeMode.GROUP_MEMBER;
    }

}
