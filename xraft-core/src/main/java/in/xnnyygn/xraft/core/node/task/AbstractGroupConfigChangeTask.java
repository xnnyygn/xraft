package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
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
    private volatile GroupConfigEntry groupConfigEntry;
    protected State state = State.START;

    AbstractGroupConfigChangeTask(GroupConfigChangeTaskContext context) {
        this.context = context;
    }

    @Override
    public synchronized GroupConfigChangeTaskResult call() throws Exception {
        logger.debug("task start");
        setState(State.START);
        appendGroupConfig();
        logger.debug("start waiting");
        wait();
        logger.debug("task done");
        context.done();
        return mapResult(state);
    }

    @Override
    public synchronized void setGroupConfigEntry(GroupConfigEntry entry) {
        logger.debug("set group config entry {}", entry);
        this.groupConfigEntry = entry;
        setState(State.GROUP_CONFIG_APPENDED);
    }

    private boolean isTargetGroupConfig(GroupConfigEntry entry) {
        return this.groupConfigEntry != null && this.groupConfigEntry.getIndex() == entry.getIndex();
    }

    @Override
    public void onLogCommitted(GroupConfigEntry entry) {
        if (isTargetGroupConfig(entry)) {
            doOnLogCommitted(entry);
        }
    }

    protected abstract void doOnLogCommitted(GroupConfigEntry entry);

    private GroupConfigChangeTaskResult mapResult(State state) {
        if (state == State.GROUP_CONFIG_COMMITTED) {
            return GroupConfigChangeTaskResult.OK;
        }
        return GroupConfigChangeTaskResult.TIMEOUT;
    }

    protected void setState(State state) {
        logger.debug("state -> {}", state);
        this.state = state;
    }

    protected abstract void appendGroupConfig();

}
