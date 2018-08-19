package in.xnnyygn.xraft.core.node.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractGroupConfigChangeTask implements GroupConfigChangeTask {

    protected enum State {
        START,
        GROUP_CONFIG_APPENDED,
        GROUP_CONFIG_COMMITTED,
        TIMEOUT
    }

    private static final Logger logger = LoggerFactory.getLogger(AbstractGroupConfigChangeTask.class);
    protected final GroupConfigChangeTaskContext context;
    protected State state = State.START;

    AbstractGroupConfigChangeTask(GroupConfigChangeTaskContext context) {
        this.context = context;
    }

    @Override
    public synchronized GroupConfigChangeTaskResult call() throws Exception {
        logger.debug("task start");
        setState(State.START);
        appendGroupConfig();
        setState(State.GROUP_CONFIG_APPENDED);
        wait();
        logger.debug("task done");
        context.done();
        return mapResult(state);

    }

    private GroupConfigChangeTaskResult mapResult(State state) {
        switch (state) {
            case GROUP_CONFIG_COMMITTED:
                return GroupConfigChangeTaskResult.OK;
            default:
                return GroupConfigChangeTaskResult.TIMEOUT;
        }
    }

    protected void setState(State state) {
        logger.debug("state -> {}", state);
        this.state = state;
    }

    protected abstract void appendGroupConfig();

    @Override
    public synchronized void onLogCommitted() {
        if (state != State.GROUP_CONFIG_APPENDED) {
            throw new IllegalStateException("log committed before log appended");
        }
        setState(State.GROUP_CONFIG_COMMITTED);
        notify();
    }

}
