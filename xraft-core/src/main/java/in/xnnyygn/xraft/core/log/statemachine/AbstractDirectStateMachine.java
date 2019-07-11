package in.xnnyygn.xraft.core.log.statemachine;

import in.xnnyygn.xraft.core.log.snapshot.Snapshot;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.role.AbstractNodeRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public abstract class AbstractDirectStateMachine implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(AbstractNodeRole.class);
    protected int lastApplied = 0;

    @Override
    public int getLastApplied() {
        return lastApplied;
    }

    @Override
    public void applyLog(StateMachineContext context, int index, int term, @Nonnull byte[] commandBytes, int firstLogIndex, Set<NodeEndpoint> lastGroupConfig) {
        logger.debug("apply log {}", index);
        applyCommand(commandBytes);
        lastApplied = index;
        if (shouldGenerateSnapshot(firstLogIndex, index)) {
            context.generateSnapshot(index);
        }
    }

    /**
     * Should generate or not.
     *
     * @param firstLogIndex first log index in log files, may not be {@code 0}
     * @param lastApplied   last applied log index
     * @return true if should generate, otherwise false
     */
    abstract boolean shouldGenerateSnapshot(int firstLogIndex, int lastApplied);

    protected abstract void applyCommand(@Nonnull byte[] commandBytes);

    @Override
    public void applySnapshot(@Nonnull Snapshot snapshot) throws IOException {
        logger.info("apply snapshot, last included index {}", snapshot.getLastIncludedIndex());
        doApplySnapshot(snapshot.getDataStream());
        lastApplied = snapshot.getLastIncludedIndex();
    }

    protected abstract void doApplySnapshot(@Nonnull InputStream input) throws IOException;

}
