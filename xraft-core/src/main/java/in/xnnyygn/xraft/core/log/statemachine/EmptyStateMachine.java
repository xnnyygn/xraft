package in.xnnyygn.xraft.core.log.statemachine;

import in.xnnyygn.xraft.core.log.snapshot.Snapshot;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;

public class EmptyStateMachine implements StateMachine {

    private int lastApplied = 0;

    @Override
    public int getLastApplied() {
        return lastApplied;
    }

    @Override
    public void applyLog(StateMachineContext context, int index, @Nonnull byte[] commandBytes, int firstLogIndex) {
        lastApplied = index;
    }

    @Override
    public boolean shouldGenerateSnapshot(int firstLogIndex, int lastApplied) {
        return false;
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
