package in.xnnyygn.xraft.core.log.statemachine;

import in.xnnyygn.xraft.core.log.snapshot.Snapshot;
import in.xnnyygn.xraft.core.node.NodeEndpoint;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

/**
 * State machine.
 */
public interface StateMachine {

    int getLastApplied();

    void applyLog(StateMachineContext context, int index, int term, @Nonnull byte[] commandBytes, int firstLogIndex, Set<NodeEndpoint> lastGroupConfig);

    void advanceLastApplied(int index);

    void onReadIndexReached(String requestId, int readIndex);

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
