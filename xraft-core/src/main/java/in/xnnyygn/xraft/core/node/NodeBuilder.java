package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.rpc.Endpoint;
import in.xnnyygn.xraft.core.rpc.nio.NioConnector;
import in.xnnyygn.xraft.core.schedule.Scheduler;

public class NodeBuilder {

    private final NodeId id;
    private final NodeGroup group;
    private final EventBus eventBus;
    private final Endpoint endpoint;

    public NodeBuilder(NodeId id, NodeGroup group) {
        this.id = id;
        this.endpoint = group.find(id).getEndpoint();
        this.group = group;
        this.eventBus = new EventBus(id.getValue());
    }

    public Node build() {
        NodeContext context = new NodeContext(id, group, new NodeStore());
        context.setEventBus(eventBus);
        context.setLog(new MemoryLog());
        context.setScheduler(new Scheduler());
        context.setConnector(new NioConnector(group, id, eventBus, endpoint.getPort()));
        return new Node(context);
    }

}
