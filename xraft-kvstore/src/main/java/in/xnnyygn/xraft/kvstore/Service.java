package in.xnnyygn.xraft.kvstore;

import in.xnnyygn.xraft.core.server.Server;
import in.xnnyygn.xraft.core.nodestate.NodeRole;
import in.xnnyygn.xraft.core.nodestate.NodeStateSnapshot;
import in.xnnyygn.xraft.core.service.NodeStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Service {

    private static final Logger logger = LoggerFactory.getLogger(Service.class);
    private final Server server;

    private final Map<String, Object> map = new HashMap<>();

    public Service(Server server) {
        this.server = server;
    }

    public void start() {
        this.server.start();

    }

    public void set(String key, Object value) {
        logger.info("Server {}, set {}", this.server.getId(), key);
        checkLeadership();
        this.map.put(key, value);
    }

    public Object get(String key) {
        logger.info("Server {}, get {}", this.server.getId(), key);
        checkLeadership();
        return this.map.get(key);
    }

    private void checkLeadership() {
        NodeStateSnapshot state = this.server.getServerState();
        if (state.getRole() == NodeRole.FOLLOWER) {
            throw new NodeStateException(NodeRole.FOLLOWER, state.getLeaderId());
        }
        if (state.getRole() == NodeRole.CANDIDATE) {
            throw new NodeStateException(NodeRole.CANDIDATE, null);
        }
    }

    public void stop() throws Exception {
        this.server.stop();
    }

}
