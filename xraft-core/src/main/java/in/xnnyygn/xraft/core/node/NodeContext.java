package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.node.config.NodeConfig;
import in.xnnyygn.xraft.core.node.store.NodeStore;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.schedule.Scheduler;
import in.xnnyygn.xraft.core.support.TaskExecutor;

/**
 * Node context.
 * <p>
 * Node context should not change after initialization. e.g {@link NodeBuilder}.
 * </p>
 */
public class NodeContext {

    private NodeId selfId;
    private NodeGroup group;
    private Log log;
    private Connector connector;
    private NodeStore store;
    private Scheduler scheduler;
    private NodeMode mode;
    private NodeConfig config;
    private EventBus eventBus;
    private TaskExecutor taskExecutor;
    private TaskExecutor groupConfigChangeTaskExecutor;

    public NodeId selfId() {
        return selfId;
    }

    public void setSelfId(NodeId selfId) {
        this.selfId = selfId;
    }

    public NodeGroup group() {
        return group;
    }

    public void setGroup(NodeGroup group) {
        this.group = group;
    }

    public Log log() {
        return log;
    }

    public void setLog(Log log) {
        this.log = log;
    }

    public Connector connector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public NodeStore store() {
        return store;
    }

    public void setStore(NodeStore store) {
        this.store = store;
    }

    public Scheduler scheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public NodeMode mode() {
        return mode;
    }

    public void setMode(NodeMode mode) {
        this.mode = mode;
    }

    public NodeConfig config() {
        return config;
    }

    public void setConfig(NodeConfig config) {
        this.config = config;
    }

    public EventBus eventBus() {
        return eventBus;
    }

    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    public TaskExecutor taskExecutor() {
        return taskExecutor;
    }

    public void setTaskExecutor(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public TaskExecutor groupConfigChangeTaskExecutor() {
        return groupConfigChangeTaskExecutor;
    }

    public void setGroupConfigChangeTaskExecutor(TaskExecutor groupConfigChangeTaskExecutor) {
        this.groupConfigChangeTaskExecutor = groupConfigChangeTaskExecutor;
    }

}
