package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO create abstract group config change task
public class AddNodeTask implements GroupConfigChangeTask {

    private enum State {
        START,
        GROUP_CONFIG_APPENDED,
        GROUP_CONFIG_COMMITTED,
        TIMEOUT
    }

    private static final Logger logger = LoggerFactory.getLogger(AddNodeTask.class);
    private final GroupConfigChangeTaskContext context;
    private final NodeEndpoint endpoint;
    private final int nextIndex;
    private final int matchIndex;
    private State state = State.START;

    public AddNodeTask(GroupConfigChangeTaskContext context, NodeEndpoint endpoint, int nextIndex, int matchIndex) {
        this.context = context;
        this.endpoint = endpoint;
        this.nextIndex = nextIndex;
        this.matchIndex = matchIndex;
    }

    @Override
    public boolean isTargetNode(NodeId nodeId) {
        return endpoint.getId().equals(nodeId);
    }

    @Override
    public synchronized GroupConfigChangeTaskResult call() throws Exception {
        logger.info("task start");
        setState(State.START);
        context.addNode(endpoint, nextIndex, matchIndex);
        setState(State.GROUP_CONFIG_APPENDED);
        wait();
        logger.info("task done");
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

    private void setState(State state) {
        logger.debug("state -> {}", state);
        this.state = state;
    }

    // in node thread
    @Override
    public synchronized void onLogCommitted() {
        if(state != State.GROUP_CONFIG_APPENDED) {
            throw new IllegalStateException("log committed before log appended");
        }
        setState(State.GROUP_CONFIG_COMMITTED);
        notify();
    }

    @Override
    public String toString() {
        return "AddNodeTask{" +
                "state=" + state +
                ", endpoint=" + endpoint +
                ", nextIndex=" + nextIndex +
                ", matchIndex=" + matchIndex +
                '}';
    }

}
