package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.node.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingNodeStateListener implements NodeStateListener {

    private static final Logger logger = LoggerFactory.getLogger(LoggingNodeStateListener.class);

    private final NodeId selfNodeId;

    // TODO remove self node id
    public LoggingNodeStateListener(NodeId selfNodeId) {
        this.selfNodeId = selfNodeId;
    }

    @Override
    public void nodeStateChanged(NodeStateSnapshot snapshot) {
        logger.info("Node {}, state changed -> {}", this.selfNodeId, snapshot);
    }

}
