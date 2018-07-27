package in.xnnyygn.xraft.core.noderole;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingNodeRoleListener implements NodeRoleListener {

    private static final Logger logger = LoggerFactory.getLogger(LoggingNodeRoleListener.class);

    @Override
    public void nodeRoleChanged(RoleStateSnapshot snapshot) {
        logger.info("state changed -> {}", snapshot);
    }

}
