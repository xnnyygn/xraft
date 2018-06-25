package in.xnnyygn.xraft.core.nodestate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingNodeStateListener implements NodeStateListener {

    private static final Logger logger = LoggerFactory.getLogger(LoggingNodeStateListener.class);

    @Override
    public void nodeStateChanged(NodeStateSnapshot snapshot) {
        logger.info("state changed -> {}", snapshot);
    }

}
