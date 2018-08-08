package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.EntrySequence;
import in.xnnyygn.xraft.core.log.entry.FileEntrySequence;
import in.xnnyygn.xraft.core.log.snapshot.*;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class FileLog extends AbstractLog {

    private final LogFiles logFiles;

    public FileLog(File baseDir, EventBus eventBus) {
        super(eventBus);
        logFiles = new LogFiles(baseDir);

        LogGeneration latestGeneration = logFiles.getLatestGeneration();
        snapshot = new EmptySnapshot();
        if (latestGeneration != null) {
            if (latestGeneration.getSnapshotFile().exists()) {
                snapshot = new FileSnapshot(latestGeneration);
            }
            FileEntrySequence fileEntrySequence = new FileEntrySequence(latestGeneration, snapshot.getLastIncludedIndex() + 1);
            fileEntrySequence.load();
            commitIndex = fileEntrySequence.getLastEntryIndexInFile();
            entrySequence = fileEntrySequence;
        } else {
            LogGeneration firstGeneration = logFiles.createFirstGeneration();
            entrySequence = new FileEntrySequence(firstGeneration, 1);
        }
    }

    @Override
    protected Snapshot generateSnapshot(Entry lastAppliedEntry) {
        LogDir logDir = logFiles.getLogDirForGenerating();
        try (FileSnapshotWriter snapshotWriter = new FileSnapshotWriter(
                logDir.getSnapshotFile(), lastAppliedEntry.getIndex(), lastAppliedEntry.getTerm())) {
            stateMachine.generateSnapshot(snapshotWriter.getOutput());
        } catch (IOException e) {
            throw new SnapshotIOException("failed to generate snapshot", e);
        }
        return new FileSnapshot(logDir);
    }

    @Override
    protected SnapshotBuilder newSnapshotBuilder(InstallSnapshotRpc firstRpc) {
        return new FileSnapshotBuilder(firstRpc, logFiles.getLogDirForInstalling());
    }

    @Override
    protected void replaceSnapshot(Snapshot newSnapshot) {
        FileSnapshot fileSnapshot = (FileSnapshot) newSnapshot;
        int lastIncludedIndex = fileSnapshot.getLastIncludedIndex();

        // move entries to new file
        int logIndexOffset = lastIncludedIndex + 1;

        List<Entry> remainingEntries = entrySequence.subList(logIndexOffset);
        if (!remainingEntries.isEmpty()) {
            EntrySequence newEntrySequence = new FileEntrySequence(fileSnapshot.getLogDir(), logIndexOffset);
            newEntrySequence.append(remainingEntries);
            newEntrySequence.commit(commitIndex);
            newEntrySequence.close();
        }

        snapshot.close();
        entrySequence.close();
        newSnapshot.close();

        LogDir generation = logFiles.rename(fileSnapshot.getLogDir(), lastIncludedIndex);
        snapshot = new FileSnapshot(generation);
        entrySequence = new FileEntrySequence(generation, logIndexOffset);
    }

}
