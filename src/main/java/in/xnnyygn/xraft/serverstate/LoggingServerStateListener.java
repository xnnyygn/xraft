package in.xnnyygn.xraft.serverstate;

import in.xnnyygn.xraft.server.ServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingServerStateListener implements ServerStateListener {

    private static final Logger logger = LoggerFactory.getLogger(LoggingServerStateListener.class);

    private final ServerId selfServerId;

    public LoggingServerStateListener(ServerId selfServerId) {
        this.selfServerId = selfServerId;
    }

    @Override
    public void serverStateChanged(ServerStateSnapshot snapshot) {
        logger.info("Server {}, state changed -> {}", this.selfServerId, snapshot);
    }

}
