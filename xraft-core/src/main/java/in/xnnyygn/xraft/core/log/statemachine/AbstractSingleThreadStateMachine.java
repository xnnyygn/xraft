package in.xnnyygn.xraft.core.log.statemachine;

import in.xnnyygn.xraft.core.log.snapshot.Snapshot;
import in.xnnyygn.xraft.core.support.SingleThreadTaskExecutor;
import in.xnnyygn.xraft.core.support.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;

public abstract class AbstractSingleThreadStateMachine implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSingleThreadStateMachine.class);
    private volatile int lastApplied = 0;
    private final TaskExecutor taskExecutor;

    public AbstractSingleThreadStateMachine() {
        taskExecutor = new SingleThreadTaskExecutor("state-machine");
    }

    @Override
    public int getLastApplied() {
        return lastApplied;
    }

    @Override
    public void applyLog(StateMachineContext context, int index, @Nonnull byte[] commandBytes, int firstLogIndex) {
        taskExecutor.submit(() -> doApplyLog(context, index, commandBytes, firstLogIndex));
    }

    private void doApplyLog(StateMachineContext context, int index, @Nonnull byte[] commandBytes, int firstLogIndex) {
        if (index <= lastApplied) {
            return;
        }
        logger.debug("apply log {}", index);
        applyCommand(commandBytes);
        lastApplied = index;
        if (shouldGenerateSnapshot(firstLogIndex, index)) {
            context.generateSnapshot(index);
        }
    }

    protected abstract void applyCommand(@Nonnull byte[] commandBytes);

    // run in node thread
    @Override
    public void applySnapshot(@Nonnull Snapshot snapshot) throws IOException {
        logger.info("apply snapshot, last included index {}", snapshot.getLastIncludedIndex());
        doApplySnapshot(snapshot.getDataStream());
        lastApplied = snapshot.getLastIncludedIndex();
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
