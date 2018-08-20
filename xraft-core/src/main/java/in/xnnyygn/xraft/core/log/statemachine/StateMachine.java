package in.xnnyygn.xraft.core.log.statemachine;

import in.xnnyygn.xraft.core.log.snapshot.Snapshot;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;

/**
 * State machine.
 */
public interface StateMachine {

    int getLastApplied();

    void applyLog(StateMachineContext context, int index, @Nonnull byte[] commandBytes, int firstLogIndex);

    /**
     * Should generate or not.
     *
     * @param firstLogIndex first log index in log files, may not be {@code 0}
     * @param lastApplied   last applied log index
     * @return true if should generate, otherwise false
     */
    boolean shouldGenerateSnapshot(int firstLogIndex, int lastApplied);

    /**
     * Generate snapshot to output.
     *
     * @param output output
     * @throws IOException if IO error occurred
     */
    void generateSnapshot(@Nonnull OutputStream output) throws IOException;

    void applySnapshot(@Nonnull Snapshot snapshot) throws IOException;

    void shutdown();

}
