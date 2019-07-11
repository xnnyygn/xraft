package in.xnnyygn.xraft.core.log.statemachine;

import in.xnnyygn.xraft.core.log.snapshot.Snapshot;
import in.xnnyygn.xraft.core.node.NodeEndpoint;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

public class EmptyStateMachine implements StateMachine {

    private int lastApplied = 0;

    @Override
    public int getLastApplied() {
        return lastApplied;
    }

    @Override
    public void applyLog(StateMachineContext context, int index, int term, @Nonnull byte[] commandBytes, int firstLogIndex, Set<NodeEndpoint> lastGroupConfig) {
        lastApplied = index;
    }

    @Override
    public void advanceLastApplied(int index) {
        lastApplied = index;
    }

    @Override
    public void onReadIndexReached(String requestId, int readIndex) {
    }

    @Override
    public void generateSnapshot(@Nonnull OutputStream output) throws IOException {
    }

    @Override
    public void applySnapshot(@Nonnull Snapshot snapshot) throws IOException {
        lastApplied = snapshot.getLastIncludedIndex();
    }

    @Override
    public void shutdown() {
    }

}
