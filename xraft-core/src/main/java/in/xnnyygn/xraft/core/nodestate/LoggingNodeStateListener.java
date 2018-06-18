package in.xnnyygn.xraft.core.nodestate;

import in.xnnyygn.xraft.core.server.ServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingNodeStateListener implements NodeStateListener {

    private static final Logger logger = LoggerFactory.getLogger(LoggingNodeStateListener.class);

    private final ServerId selfServerId;

    public LoggingNodeStateListener(ServerId selfServerId) {
        this.selfServerId = selfServerId;
    }

    @Override
    public void nodeStateChanged(NodeStateSnapshot snapshot) {
        logger.info("Server {}, state changed -> {}", this.selfServerId, snapshot);
    }

}
