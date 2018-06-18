package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.node.Node;
import in.xnnyygn.xraft.core.nodestate.NodeRole;
import in.xnnyygn.xraft.core.nodestate.NodeStateSnapshot;
import in.xnnyygn.xraft.core.service.NodeStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Service {

    private static final Logger logger = LoggerFactory.getLogger(Service.class);
    private final Node node;

    private final Map<String, Object> map = new HashMap<>();

    public Service(Node node) {
        this.node = node;
    }

    public void start() {
        this.node.start();

    }

    public void set(String key, Object value) {
        logger.info("Node {}, set {}", this.node.getId(), key);
        checkLeadership();
        this.map.put(key, value);
    }

    public Object get(String key) {
        logger.info("Node {}, get {}", this.node.getId(), key);
        checkLeadership();
        return this.map.get(key);
    }

    private void checkLeadership() {
        NodeStateSnapshot state = this.node.getNodeState();
        if (state.getRole() == NodeRole.FOLLOWER) {
            throw new NodeStateException(NodeRole.FOLLOWER, state.getLeaderId());
        }
        if (state.getRole() == NodeRole.CANDIDATE) {
            throw new NodeStateException(NodeRole.CANDIDATE, null);
        }
    }

    public void stop() throws Exception {
        this.node.stop();
    }

}
