package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResultMessage;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AddNodeTask implements GroupConfigChangeTask {

    private enum State {
        START,
        REPLICATING,
        REPLICATION_FAILED,
        REPLICATION_CATCH_UP,
        GROUP_CONFIG_APPENDED,
        GROUP_CONFIG_COMMITTED,
        TIMEOUT
    }

    private static final Logger logger = LoggerFactory.getLogger(AddNodeTask.class);
    private final NodeEndpoint endpoint;
    private final NodeConfig config;
    private final GroupConfigChangeTaskContext context;
    private final long startTime;
    private State state = State.START;
    private int nextIndex = 0; // set when receive append entries result
    private int matchIndex = 0;
    private int round = 1;

    AddNodeTask(NodeEndpoint endpoint, NodeConfig config, GroupConfigChangeTaskContext context) {
        this.endpoint = endpoint;
        this.config = config;
        this.context = context;
        startTime = System.currentTimeMillis();
    }

    @Override
    public boolean isTargetNode(NodeId nodeId) {
        return endpoint.getId().equals(nodeId);
    }

    @Override
    public long getStartTime() {
        return startTime;
    }

    @Override
    public synchronized GroupConfigChangeTaskResult call() throws Exception {
        logger.info("task start");
        setState(State.START);
        context.replicateLog(endpoint);
        state = State.REPLICATING;
        wait(config.getNewNodeTimeout());
        logger.info("task done");
        context.taskDone();
        return mapResult(state);
    }

    private GroupConfigChangeTaskResult mapResult(State state) {
        switch (state) {
            case GROUP_CONFIG_COMMITTED:
                return GroupConfigChangeTaskResult.OK;
            case REPLICATION_FAILED:
                return GroupConfigChangeTaskResult.REPLICATION_FAILED;
            default:
                return GroupConfigChangeTaskResult.TIMEOUT;
        }
    }

    private void setState(State state) {
        logger.debug("state -> {}", state);
        this.state = state;
    }

    // TODO add test
    // in node thread
    public synchronized void onReceiveAppendEntriesResult(AppendEntriesResultMessage resultMessage, int nextLogIndex) {
        if (state != State.REPLICATING) {
            throw new IllegalStateException("receive append entries result when state is not replicating");
        }
        if (System.currentTimeMillis() - startTime >= config.getNewNodeTimeout()) {
            logger.debug("node {} cannot catch up within time", endpoint.getId());
            setStateAndNotify(State.TIMEOUT);
            return;
        }
        // initialize nextIndex
        if (nextIndex == 0) {
            nextIndex = nextLogIndex;
        }
        AppendEntriesRpc rpc = resultMessage.getRpc();
        if (resultMessage.get().isSuccess()) {
            logger.debug("replication state of new node {}, next index {}, match index {}", endpoint.getId(), nextIndex, matchIndex);
            int lastEntryIndex = rpc.getLastEntryIndex();
            matchIndex = lastEntryIndex;
            nextIndex = lastEntryIndex + 1;
            if (nextIndex >= nextLogIndex) { // catch up
                setState(State.REPLICATION_CATCH_UP);
                context.doAddNode(endpoint, nextIndex, matchIndex);
                setState(State.GROUP_CONFIG_APPENDED);
                return;
            }
        } else {
            if (nextIndex > 1) {
                nextIndex--;
            } else {
                logger.warn("node {} cannot back off next index more, stop replication", endpoint.getId());
                setStateAndNotify(State.REPLICATION_FAILED);
                return;
            }
        }
        if ((++round) > config.getNewNodeMaxRound()) {
            logger.info("node {} cannot catch up within max round", endpoint.getId());
            setStateAndNotify(State.TIMEOUT);
            return;
        }
        context.doReplicateLog(endpoint, nextIndex);
    }

    private void setStateAndNotify(State state) {
        setState(state);
        notify();
    }

    // in node thread
    @Override
    public synchronized void onLogCommitted() {
        setStateAndNotify(State.GROUP_CONFIG_COMMITTED);
    }

    @Override
    public String toString() {
        return "AddNodeTask{" +
                "state=" + state +
                ", matchIndex=" + matchIndex +
                ", nextIndex=" + nextIndex +
                ", round=" + round +
                ", startTime=" + startTime +
                '}';
    }

}
