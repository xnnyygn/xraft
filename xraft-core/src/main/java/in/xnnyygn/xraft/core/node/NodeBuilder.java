package in.xnnyygn.xraft.core.node;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.MemoryLog;
import in.xnnyygn.xraft.core.nodestate.LoggingNodeStateListener;
import in.xnnyygn.xraft.core.nodestate.NodeStateMachine;
import in.xnnyygn.xraft.core.rpc.Connector;
import in.xnnyygn.xraft.core.rpc.Endpoint;
import in.xnnyygn.xraft.core.rpc.embedded.EmbeddedConnector;
import in.xnnyygn.xraft.core.rpc.embedded.EmbeddedEndpoint;
import in.xnnyygn.xraft.core.rpc.nio.NioConnector;
import in.xnnyygn.xraft.core.rpc.socket.SocketEndpoint;
import in.xnnyygn.xraft.core.schedule.Scheduler;

public class NodeBuilder {

    private final NodeId id;
    private final NodeGroup group;
    private final EventBus eventBus;
    private Endpoint endpoint;

    public NodeBuilder(String id, NodeGroup group) {
        this.id = new NodeId(id);
        this.group = group;
        this.eventBus = new EventBus(id);
    }

    public NodeBuilder setSocketEndpoint(String host, int port) {
        return this.setSocketEndpoint(endpoint);
    }

    public NodeBuilder setSocketEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public Node build() {
        NodeContext context = new NodeContext(id, group, new NodeStore());
        context.setEventBus(eventBus);
        context.setLog(new MemoryLog());
        context.setScheduler(new Scheduler(id));
        context.setConnector(this.createConnector());

        NodeStateMachine stateMachine = new NodeStateMachine(context);
        stateMachine.addNodeStateListener(new LoggingNodeStateListener());
        Node node = new Node(context, stateMachine, this.createEndpoint());
        this.group.add(node);
        return node;
    }

    private Connector createConnector() {
        if (this.endpoint == null) {
            return new EmbeddedConnector(this.group, this.id);
        }
        if (this.endpoint instanceof SocketEndpoint) {
            return new NioConnector(this.group, this.id, this.eventBus, ((SocketEndpoint) this.endpoint).getPort());
        }
        throw new IllegalStateException("failed to create connector");
    }

    private Endpoint createEndpoint() {
        if (this.endpoint != null) return endpoint;

        return new EmbeddedEndpoint(this.eventBus);
    }

}
