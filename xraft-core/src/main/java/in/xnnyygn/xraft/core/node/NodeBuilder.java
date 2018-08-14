package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.FileLog;
import in.xnnyygn.xraft.core.log.Log;
import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.rpc.Address;
import in.xnnyygn.xraft.core.rpc.nio.NioConnector;
import in.xnnyygn.xraft.core.rpc.nio.NioConnectorContext;
import in.xnnyygn.xraft.core.schedule.DefaultScheduler;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.File;

@Deprecated
public class NodeBuilder {

    private final NodeId id;
    private final NodeGroup group;
    private final EventBus eventBus;
    private final Address address;
    private boolean standbyMode = false;
    private Log log;
    private NodeStore store;

    public NodeBuilder(NodeId id, NodeGroup group) {
        this.id = id;
        this.address = group.findAddress(id);
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
        store = new FileNodeStore(new File(dataDir, FileNodeStore.FILE_NAME));
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
        context.setScheduler(new DefaultScheduler());
        context.setConnector(new NioConnector(buildNioConnectorContext()));
        return new NodeImpl(context);
    }

    private NioConnectorContext buildNioConnectorContext() {
        NioConnectorContext context = new NioConnectorContext();
        context.setEventBus(eventBus);
        context.setNodeGroup(group);
        context.setSelfNodeId(id);
        context.setPort(address.getPort());
        context.setWorkerGroupShared(false);
        context.setWorkerNioEventLoopGroup(new NioEventLoopGroup(4));
        return context;
    }

}
