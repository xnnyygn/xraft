package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.nodestate.LoggingNodeStateListener;
import in.xnnyygn.xraft.core.nodestate.NodeStateMachine;
import in.xnnyygn.xraft.core.rpc.DefaultConnector;
import in.xnnyygn.xraft.core.rpc.EmbeddedChannel;
import in.xnnyygn.xraft.core.schedule.Scheduler;

public class NodeBuilder {

    private final String id;
    private final NodeGroup group;

    public NodeBuilder(String id, NodeGroup group) {
        this.id = id;
        this.group = group;
    }

    public Node build() {
        NodeId nodeId = new NodeId(id);
        EventBus eventBus = new EventBus(id);

        NodeContext context = new NodeContext(nodeId, group, new NodeStore());
        context.setEventBus(eventBus);
        context.setLog(new MemoryLog());
        context.setScheduler(new Scheduler(nodeId));
        context.setConnector(new DefaultConnector(group, nodeId));

        NodeStateMachine stateMachine = new NodeStateMachine(context);
        stateMachine.addNodeStateListener(new LoggingNodeStateListener());
        Node node = new Node(context, stateMachine, new EmbeddedChannel(nodeId, eventBus));
        this.group.add(node);
        return node;
    }

}
