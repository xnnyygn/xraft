package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.FileLog;
import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.rpc.Endpoint;
import in.xnnyygn.xraft.core.rpc.nio.NioConnector;
import in.xnnyygn.xraft.core.schedule.Scheduler;

import java.io.File;

public class NodeBuilder {

    private final NodeId id;
    private final NodeGroup group;
    private final EventBus eventBus;
    private final Endpoint endpoint;
    private boolean standbyMode = false;
    private Log log;
    private NodeStore store;

    public NodeBuilder(NodeId id, NodeGroup group) {
        this.id = id;
        this.endpoint = group.getEndpoint(id);
        this.group = group;
        this.eventBus = new EventBus(id.getValue());
    }

    public NodeBuilder setStandbyMode(boolean standbyMode) {
        this.standbyMode = standbyMode;
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
        FileNodeStore fileNodeStore = new FileNodeStore(new File(dataDir, FileNodeStore.FILE_NAME));
        fileNodeStore.initialize();
        store = fileNodeStore;
        return this;
    }

    public Node build() {
        if (store == null) {
            store = new MemoryNodeStore();
        }
        if (log == null) {
            log = new MemoryLog(eventBus);
        }

        NodeContext context = new NodeContext(id, group, store, eventBus);
        context.setStandbyMode(standbyMode);
        context.setLog(log);
        context.setScheduler(new Scheduler());
        context.setConnector(new NioConnector(group, id, eventBus, endpoint.getPort()));
        return new Node(context);
    }

}
