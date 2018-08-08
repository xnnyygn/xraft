package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.EntrySequence;
import in.xnnyygn.xraft.core.log.entry.MemoryEntrySequence;
import in.xnnyygn.xraft.core.log.snapshot.*;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MemoryLog extends AbstractLog {

    public MemoryLog() {
        this(new EventBus());
    }

    public MemoryLog(EventBus eventBus) {
        this(new EmptySnapshot(), new MemoryEntrySequence(), eventBus);
    }

    public MemoryLog(Snapshot snapshot, EntrySequence entrySequence, EventBus eventBus) {
        super(eventBus);
        this.snapshot = snapshot;
        this.entrySequence = entrySequence;
    }

    @Override
    protected Snapshot generateSnapshot(Entry lastAppliedEntry) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            stateMachine.generateSnapshot(output);
        } catch (IOException e) {
            throw new SnapshotIOException(e);
        }
        return new MemorySnapshot(lastAppliedEntry.getIndex(), lastAppliedEntry.getTerm(), output.toByteArray());
    }

    @Override
    protected SnapshotBuilder newSnapshotBuilder(InstallSnapshotRpc firstRpc) {
        return new MemorySnapshotBuilder(firstRpc);
    }

    @Override
    protected void replaceSnapshot(Snapshot newSnapshot) {
        snapshot = newSnapshot;

        int lastIncludedIndex = newSnapshot.getLastIncludedIndex();
        EntrySequence newEntrySequence = new MemoryEntrySequence(lastIncludedIndex + 1);
        newEntrySequence.append(entrySequence.subList(lastIncludedIndex + 1));
        entrySequence = newEntrySequence;
    }

}
