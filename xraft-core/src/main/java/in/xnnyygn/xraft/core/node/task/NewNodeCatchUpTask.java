package in.xnnyygn.xraft.core.node.task;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.node.config.NodeConfig;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesResultMessage;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotResultMessage;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class NewNodeCatchUpTask implements Callable<NewNodeCatchUpTaskResult> {

    private enum State {
        START,
        REPLICATING,
        REPLICATION_FAILED,
        REPLICATION_CATCH_UP,
        TIMEOUT
    }

    private static final Logger logger = LoggerFactory.getLogger(NewNodeCatchUpTask.class);
    private final NewNodeCatchUpTaskContext context;
    private final NodeEndpoint endpoint;
    private final NodeId nodeId;
    private final NodeConfig config;
    private State state = State.START;
    private boolean done = false;
    private long lastReplicateAt; // set when start
    private long lastAdvanceAt; // set when start
    private int round = 1;
    private int nextIndex = 0; // reset when receive append entries result
    private int matchIndex = 0;

    public NewNodeCatchUpTask(NewNodeCatchUpTaskContext context, NodeEndpoint endpoint, NodeConfig config) {
        this.context = context;
        this.endpoint = endpoint;
        this.nodeId = endpoint.getId();
        this.config = config;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    @Override
    public synchronized NewNodeCatchUpTaskResult call() throws Exception {
        logger.debug("task start");
        setState(State.START);
        context.replicateLog(endpoint);
        lastReplicateAt = System.currentTimeMillis();
        lastAdvanceAt = lastReplicateAt;
        setState(State.REPLICATING);
        while (!done) {
            wait(config.getNewNodeReadTimeout());
            // 1. done
            // 2. replicate -> no response within timeout
            if (System.currentTimeMillis() - lastReplicateAt >= config.getNewNodeReadTimeout()) {
                logger.debug("node {} not response within read timeout", endpoint.getId());
                state = State.TIMEOUT;
                break;
            }
        }
        logger.debug("task done");
        context.done(this);
        return mapResult(state);
    }

    private NewNodeCatchUpTaskResult mapResult(State state) {
        switch (state) {
            case REPLICATION_CATCH_UP:
                return new NewNodeCatchUpTaskResult(nextIndex, matchIndex);
            case REPLICATION_FAILED:
                return new NewNodeCatchUpTaskResult(NewNodeCatchUpTaskResult.State.REPLICATION_FAILED);
            default:
                return new NewNodeCatchUpTaskResult(NewNodeCatchUpTaskResult.State.TIMEOUT);
        }
    }

    private void setState(State state) {
        logger.debug("state -> {}", state);
        this.state = state;
    }

    // in node thread
    synchronized void onReceiveAppendEntriesResult(AppendEntriesResultMessage resultMessage, int nextLogIndex) {
        assert nodeId.equals(resultMessage.getSourceNodeId());
        if (state != State.REPLICATING) {
            throw new IllegalStateException("receive append entries result when state is not replicating");
        }
        // initialize nextIndex
        if (nextIndex == 0) {
            nextIndex = nextLogIndex;
        }
        logger.debug("replication state of new node {}, next index {}, match index {}", nodeId, nextIndex, matchIndex);
        if (resultMessage.get().isSuccess()) {
            int lastEntryIndex = resultMessage.getRpc().getLastEntryIndex();
            assert lastEntryIndex >= 0;
            matchIndex = lastEntryIndex;
            nextIndex = lastEntryIndex + 1;
            lastAdvanceAt = System.currentTimeMillis();
            if (nextIndex >= nextLogIndex) { // catch up
                setStateAndNotify(State.REPLICATION_CATCH_UP);
                return;
            }
            if ((++round) > config.getNewNodeMaxRound()) {
                logger.info("node {} cannot catch up within max round", nodeId);
                setStateAndNotify(State.TIMEOUT);
                return;
            }
        } else {
            if (nextIndex <= 1) {
                logger.warn("node {} cannot back off next index more, stop replication", nodeId);
                setStateAndNotify(State.REPLICATION_FAILED);
                return;
            }
            nextIndex--;
            if (System.currentTimeMillis() - lastAdvanceAt >= config.getNewNodeAdvanceTimeout()) {
                logger.debug("node {} cannot make progress within timeout", nodeId);
                setStateAndNotify(State.TIMEOUT);
                return;
            }
        }
        context.doReplicateLog(endpoint, nextIndex);
        lastReplicateAt = System.currentTimeMillis();
        notify();
    }

    // in node thread
    synchronized void onReceiveInstallSnapshotResult(InstallSnapshotResultMessage resultMessage, int nextLogIndex) {
        assert nodeId.equals(resultMessage.getSourceNodeId());
        if (state != State.REPLICATING) {
            throw new IllegalStateException("receive append entries result when state is not replicating");
        }
        InstallSnapshotRpc rpc = resultMessage.getRpc();
        if (rpc.isDone()) {
            matchIndex = rpc.getLastIndex();
            nextIndex = rpc.getLastIndex() + 1;
            lastAdvanceAt = System.currentTimeMillis();
            if (nextIndex >= nextLogIndex) {
                setStateAndNotify(State.REPLICATION_CATCH_UP);
                return;
            }
            round++;
            context.doReplicateLog(endpoint, nextIndex);
        } else {
            context.sendInstallSnapshot(endpoint, rpc.getOffset() + rpc.getDataLength());
        }
        lastReplicateAt = System.currentTimeMillis();
        notify();
    }

    private void setStateAndNotify(State state) {
        setState(state);
        done = true;
        notify();
    }

    @Override
    public String toString() {
        return "NewNodeCatchUpTask{" +
                "state=" + state +
                ", endpoint=" + endpoint +
                ", done=" + done +
                ", lastReplicateAt=" + lastReplicateAt +
                ", lastAdvanceAt=" + lastAdvanceAt +
                ", nextIndex=" + nextIndex +
                ", matchIndex=" + matchIndex +
                ", round=" + round +
                '}';
    }

}
