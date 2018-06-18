package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.nodestate.NodeStateMachine;
import in.xnnyygn.xraft.core.rpc.Channel;
import in.xnnyygn.xraft.core.nodestate.NodeStateSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node extends AbstractNode {

    private static final Logger logger = LoggerFactory.getLogger(Node.class);
    private final NodeStateMachine nodeStateMachine;
    private final Channel rpcChannel;

    public Node(NodeId id, NodeStateMachine nodeStateMachine, Channel rpcChannel) {
        super(id);
        this.nodeStateMachine = nodeStateMachine;
        this.rpcChannel = rpcChannel;
    }

    public void start() {
        logger.info("start node {}", getId());
        this.nodeStateMachine.start();
    }

    public NodeStateSnapshot getServerState() {
        return this.nodeStateMachine.takeSnapshot();
    }

    // TODO internal channel, change to package visible
    public Channel getRpcChannel() {
        return this.rpcChannel;
    }

    public void stop() throws Exception {
        logger.info("stop node {}", getId());
        this.nodeStateMachine.stop();
        this.rpcChannel.close();
    }

}
