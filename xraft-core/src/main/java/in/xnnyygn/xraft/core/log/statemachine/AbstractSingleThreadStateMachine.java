package in.xnnyygn.xraft.core.log.statemachine;

import in.xnnyygn.xraft.core.log.snapshot.Snapshot;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.support.SingleThreadTaskExecutor;
import in.xnnyygn.xraft.core.support.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

public abstract class AbstractSingleThreadStateMachine implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSingleThreadStateMachine.class);
    private volatile int lastApplied = 0;
    private final TaskExecutor taskExecutor;
    private final SortedMap<Integer, List<String>> readIndexMap = new TreeMap<>();

    public AbstractSingleThreadStateMachine() {
        taskExecutor = new SingleThreadTaskExecutor("state-machine");
    }

    @Override
    public int getLastApplied() {
        return lastApplied;
    }

    @Override
    public void applyLog(StateMachineContext context, int index, int term, @Nonnull byte[] commandBytes, int firstLogIndex, Set<NodeEndpoint> lastGroupConfig) {
        taskExecutor.submit(() -> doApplyLog(context, index, term, commandBytes, firstLogIndex, lastGroupConfig));
    }

    private void doApplyLog(StateMachineContext context, int index, int term, @Nonnull byte[] commandBytes, int firstLogIndex, Set<NodeEndpoint> lastGroupConfig) {
        if (index <= lastApplied) {
            return;
        }
        logger.debug("apply log {}", index);
        applyCommand(commandBytes);
        lastApplied = index;
        onLastAppliedAdvanced();
        if (!shouldGenerateSnapshot(firstLogIndex, index)) {
            return;
        }
        try {
            OutputStream output = context.getOutputForGeneratingSnapshot(index, term, lastGroupConfig);
            generateSnapshot(output);
            context.doneGeneratingSnapshot(index);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void advanceLastApplied(int index) {
        taskExecutor.submit(() -> {
            if (index <= lastApplied) {
                return;
            }
            lastApplied = index;
            onLastAppliedAdvanced();
        });
    }

    private void onLastAppliedAdvanced() {
        logger.debug("last applied index advanced, {}", lastApplied);
        SortedMap<Integer, List<String>> subMap = readIndexMap.headMap(lastApplied + 1);
        for (List<String> requestIds : subMap.values()) {
            for (String requestId : requestIds) {
                onReadIndexReached(requestId);
            }
        }
        subMap.clear();
    }

    /**
     * Should generate or not.
     *
     * @param firstLogIndex first log index in log files, may not be {@code 0}
     * @param lastApplied   last applied log index
     * @return true if should generate, otherwise false
     */
    public abstract boolean shouldGenerateSnapshot(int firstLogIndex, int lastApplied);

    protected abstract void applyCommand(@Nonnull byte[] commandBytes);

    @Override
    public void onReadIndexReached(String requestId, int readIndex) {
        logger.debug("read index reached, request id {}, read index {}", requestId, readIndex);
        taskExecutor.submit(() -> {
            if (lastApplied >= readIndex) {
                onReadIndexReached(requestId);
            } else {
                logger.debug("waiting for last applied index {} to reach read index {}", lastApplied, readIndex);
                List<String> requestIds = readIndexMap.get(readIndex);
                if (requestIds == null) {
                    requestIds = new LinkedList<>();
                }
                requestIds.add(requestId);
                readIndexMap.put(readIndex, requestIds);
            }
        });
    }

    protected abstract void onReadIndexReached(String requestId);

    // run in node thread
    @Override
    public void applySnapshot(@Nonnull Snapshot snapshot) throws IOException {
        taskExecutor.submit(() -> {
            logger.info("apply snapshot, last included index {}", snapshot.getLastIncludedIndex());
            try {
                doApplySnapshot(snapshot.getDataStream());
                lastApplied = snapshot.getLastIncludedIndex();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    protected abstract void doApplySnapshot(@Nonnull InputStream input) throws IOException;

    @Override
    public void shutdown() {
        try {
            taskExecutor.shutdown();
        } catch (InterruptedException e) {
            throw new StateMachineException(e);
        }
    }

}
