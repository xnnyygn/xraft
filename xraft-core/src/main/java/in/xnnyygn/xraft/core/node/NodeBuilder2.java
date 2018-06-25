package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.nodestate.LoggingNodeStateListener;
import in.xnnyygn.xraft.core.nodestate.NodeStateMachine2;
import in.xnnyygn.xraft.core.rpc.DefaultConnector;
import in.xnnyygn.xraft.core.rpc.EmbeddedChannel2;
import in.xnnyygn.xraft.core.schedule.Scheduler;

public class NodeBuilder2 {

    private final String id;
    private final NodeGroup group;

    public NodeBuilder2(String id, NodeGroup group) {
        this.id = id;
        this.group = group;
    }

    public Node2 build() {
        NodeId nodeId = new NodeId(id);
        EventBus eventBus = new EventBus(id);

        NodeContext context = new NodeContext(nodeId, group, new NodeStore());
        context.setEventBus(eventBus);
        context.setLog(new MemoryLog(eventBus));
        context.setScheduler(new Scheduler(nodeId));
        context.setConnector(new DefaultConnector(group, nodeId));

        NodeStateMachine2 stateMachine = new NodeStateMachine2(context);
        stateMachine.addNodeStateListener(new LoggingNodeStateListener(nodeId));
        Node2 node = new Node2(context, stateMachine, new EmbeddedChannel2(nodeId, eventBus));
        this.group.add(node);
        return node;
    }

}
