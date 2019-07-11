package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.EntryMeta;
import in.xnnyygn.xraft.core.log.event.SnapshotGeneratedEvent;
import in.xnnyygn.xraft.core.log.sequence.EntrySequence;
import in.xnnyygn.xraft.core.log.sequence.FileEntrySequence;
import in.xnnyygn.xraft.core.log.snapshot.*;
import in.xnnyygn.xraft.core.log.statemachine.StateMachineContext;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

@NotThreadSafe
public class FileLog extends AbstractLog {

    private final RootDir rootDir;

    public FileLog(File baseDir, EventBus eventBus, Set<NodeEndpoint> baseGroup) {
        super(eventBus);
        setStateMachineContext(new StateMachineContextImpl());
        rootDir = new RootDir(baseDir);

        LogGeneration latestGeneration = rootDir.getLatestGeneration();
        snapshot = new EmptySnapshot();
        // TODO add log
        if (latestGeneration != null) {
            Set<NodeEndpoint> initialGroup = baseGroup;
            if (latestGeneration.getSnapshotFile().exists()) {
                snapshot = new FileSnapshot(latestGeneration);
                initialGroup = snapshot.getLastConfig();
            }
            FileEntrySequence fileEntrySequence = new FileEntrySequence(latestGeneration, snapshot.getLastIncludedIndex() + 1);
            commitIndex = fileEntrySequence.getCommitIndex();
            entrySequence = fileEntrySequence;
            // TODO apply last group config entry
            groupConfigEntryList = entrySequence.buildGroupConfigEntryList(initialGroup);
        } else {
            LogGeneration firstGeneration = rootDir.createFirstGeneration();
            entrySequence = new FileEntrySequence(firstGeneration, 1);
        }
    }

    @Override
    protected Snapshot generateSnapshot(EntryMeta lastAppliedEntryMeta, Set<NodeEndpoint> groupConfig) {
        LogDir logDir = rootDir.getLogDirForGenerating();
        try (SnapshotWriter snapshotWriter = new FileSnapshotWriter(
                logDir.getSnapshotFile(), lastAppliedEntryMeta.getIndex(), lastAppliedEntryMeta.getTerm(), groupConfig)) {
            stateMachine.generateSnapshot(snapshotWriter.getOutput());
        } catch (Exception e) {
            throw new LogException("failed to generate snapshot", e);
        }
        return new FileSnapshot(logDir);
    }

    @Override
    public void snapshotGenerated(int lastIncludedIndex) {
        if (lastIncludedIndex <= snapshot.getLastIncludedIndex()) {
            return;
        }
        replaceSnapshot(new FileSnapshot(rootDir.getLogDirForGenerating()));
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

        List<Entry> remainingEntries = entrySequence.subView(logIndexOffset);
        EntrySequence newEntrySequence = new FileEntrySequence(fileSnapshot.getLogDir(), logIndexOffset);
        newEntrySequence.append(remainingEntries);
        newEntrySequence.commit(Math.max(commitIndex, lastIncludedIndex));
        newEntrySequence.close();

        snapshot.close();
        entrySequence.close();
        newSnapshot.close();

        LogDir generation = rootDir.rename(fileSnapshot.getLogDir(), lastIncludedIndex);
        snapshot = new FileSnapshot(generation);
        entrySequence = new FileEntrySequence(generation, logIndexOffset);
        groupConfigEntryList = entrySequence.buildGroupConfigEntryList(snapshot.getLastConfig());
        commitIndex = entrySequence.getCommitIndex();
    }

    private class StateMachineContextImpl implements StateMachineContext {

        private FileSnapshotWriter snapshotWriter = null;

        @Override
        public void generateSnapshot(int lastIncludedIndex) {
            throw new UnsupportedOperationException();
        }

        public OutputStream getOutputForGeneratingSnapshot(int lastIncludedIndex, int lastIncludedTerm, Set<NodeEndpoint> groupConfig) throws Exception {
            if (snapshotWriter != null) {
                snapshotWriter.close();
            }
            snapshotWriter = new FileSnapshotWriter(rootDir.getLogDirForGenerating().getSnapshotFile(), lastIncludedIndex, lastIncludedTerm, groupConfig);
            return snapshotWriter.getOutput();
        }

        public void doneGeneratingSnapshot(int lastIncludedIndex) throws Exception {
            if (snapshotWriter == null) {
                throw new IllegalStateException("snapshot not created");
            }
            snapshotWriter.close();
            eventBus.post(new SnapshotGeneratedEvent(lastIncludedIndex));
        }
    }
}
