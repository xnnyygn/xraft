package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.EntryMeta;
import in.xnnyygn.xraft.core.log.sequence.EntrySequence;
import in.xnnyygn.xraft.core.log.sequence.FileEntrySequence;
import in.xnnyygn.xraft.core.log.snapshot.*;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class FileLog extends AbstractLog {

    private final RootDir rootDir;

    public FileLog(File baseDir, EventBus eventBus) {
        super(eventBus);
        rootDir = new RootDir(baseDir);

        LogGeneration latestGeneration = rootDir.getLatestGeneration();
        snapshot = new EmptySnapshot();
        if (latestGeneration != null) {
            if (latestGeneration.getSnapshotFile().exists()) {
                snapshot = new FileSnapshot(latestGeneration);
            }
            FileEntrySequence fileEntrySequence = new FileEntrySequence(latestGeneration, snapshot.getLastIncludedIndex() + 1);
            commitIndex = fileEntrySequence.getCommitIndex();
            entrySequence = fileEntrySequence;
            // TODO apply last group config entry
            groupConfigEntryList = entrySequence.buildGroupConfigEntryList();
        } else {
            LogGeneration firstGeneration = rootDir.createFirstGeneration();
            entrySequence = new FileEntrySequence(firstGeneration, 1);
        }
    }

    @Override
    protected Snapshot generateSnapshot(EntryMeta lastAppliedEntryMeta) {
        LogDir logDir = rootDir.getLogDirForGenerating();
        try (FileSnapshotWriter snapshotWriter = new FileSnapshotWriter(
                logDir.getSnapshotFile(), lastAppliedEntryMeta.getIndex(), lastAppliedEntryMeta.getTerm())) {
            stateMachine.generateSnapshot(snapshotWriter.getOutput());
        } catch (IOException e) {
            throw new LogException("failed to generate snapshot", e);
        }
        return new FileSnapshot(logDir);
    }

    @Override
    protected SnapshotBuilder newSnapshotBuilder(InstallSnapshotRpc firstRpc) {
        return new FileSnapshotBuilder(firstRpc, rootDir.getLogDirForInstalling());
    }

    @Override
    protected void replaceSnapshot(Snapshot newSnapshot) {
        FileSnapshot fileSnapshot = (FileSnapshot) newSnapshot;
        int lastIncludedIndex = fileSnapshot.getLastIncludedIndex();
        int logIndexOffset = lastIncludedIndex + 1;
        if (!entrySequence.isEmpty()) {
            List<Entry> remainingEntries = entrySequence.subList(logIndexOffset);
            EntrySequence newEntrySequence = new FileEntrySequence(fileSnapshot.getLogDir(), logIndexOffset);
            newEntrySequence.append(remainingEntries);
            newEntrySequence.commit(commitIndex);
            newEntrySequence.close();
        }
        snapshot.close();
        entrySequence.close();
        newSnapshot.close();

        LogDir generation = rootDir.rename(fileSnapshot.getLogDir(), lastIncludedIndex);
        snapshot = new FileSnapshot(generation);
        entrySequence = new FileEntrySequence(generation, logIndexOffset);
    }

}
