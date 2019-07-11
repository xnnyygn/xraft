package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.entry.*;
import in.xnnyygn.xraft.core.log.sequence.EntrySequence;
import in.xnnyygn.xraft.core.log.sequence.GroupConfigEntryList;
import in.xnnyygn.xraft.core.log.snapshot.*;
import in.xnnyygn.xraft.core.log.statemachine.EmptyStateMachine;
import in.xnnyygn.xraft.core.log.statemachine.StateMachine;
import in.xnnyygn.xraft.core.log.statemachine.StateMachineContext;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;

abstract class AbstractLog implements Log {

    private static final Logger logger = LoggerFactory.getLogger(AbstractLog.class);

    protected final EventBus eventBus;
    protected Snapshot snapshot;
    protected EntrySequence entrySequence;

    protected SnapshotBuilder snapshotBuilder = new NullSnapshotBuilder();
    protected GroupConfigEntryList groupConfigEntryList;
    private StateMachineContext stateMachineContext;
    protected StateMachine stateMachine = new EmptyStateMachine();
    protected int commitIndex = 0;

    AbstractLog(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    void setStateMachineContext(StateMachineContext stateMachineContext) {
        this.stateMachineContext = stateMachineContext;
    }

    @Override
    @Nonnull
    public EntryMeta getLastEntryMeta() {
        if (entrySequence.isEmpty()) {
            return new EntryMeta(Entry.KIND_NO_OP, snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm());
        }
        return entrySequence.getLastEntry().getMeta();
    }

    @Override
    public AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfId, int nextIndex, int maxEntries) {
        int nextLogIndex = entrySequence.getNextLogIndex();
        if (nextIndex > nextLogIndex) {
            throw new IllegalArgumentException("illegal next index " + nextIndex);
        }
        if (nextIndex <= snapshot.getLastIncludedIndex()) {
            throw new EntryInSnapshotException(nextIndex);
        }
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setMessageId(UUID.randomUUID().toString());
        rpc.setTerm(term);
        rpc.setLeaderId(selfId);
        rpc.setLeaderCommit(commitIndex);
        if (nextIndex == snapshot.getLastIncludedIndex() + 1) {
            rpc.setPrevLogIndex(snapshot.getLastIncludedIndex());
            rpc.setPrevLogTerm(snapshot.getLastIncludedTerm());
        } else {
            // if entry sequence is empty,
            // snapshot.lastIncludedIndex + 1 == nextLogIndex,
            // so it has been rejected at the first line.
            //
            // if entry sequence is not empty,
            // snapshot.lastIncludedIndex + 1 < nextIndex <= nextLogIndex
            // and snapshot.lastIncludedIndex + 1 = firstLogIndex
            //     nextLogIndex = lastLogIndex + 1
            // then firstLogIndex < nextIndex <= lastLogIndex + 1
            //      firstLogIndex + 1 <= nextIndex <= lastLogIndex + 1
            //      firstLogIndex <= nextIndex - 1 <= lastLogIndex
            // it is ok to get entry without null check
            Entry entry = entrySequence.getEntry(nextIndex - 1);
            assert entry != null;
            rpc.setPrevLogIndex(entry.getIndex());
            rpc.setPrevLogTerm(entry.getTerm());
        }
        if (!entrySequence.isEmpty()) {
            int maxIndex = (maxEntries == ALL_ENTRIES ? nextLogIndex : Math.min(nextLogIndex, nextIndex + maxEntries));
            rpc.setEntries(entrySequence.subList(nextIndex, maxIndex));
        }
        return rpc;
    }

    @Override
    public InstallSnapshotRpc createInstallSnapshotRpc(int term, NodeId selfId, int offset, int length) {
        return createInstallSnapshotRpc(term, snapshot.getLastIncludedIndex(), selfId, offset, length);
    }

    @Override
    public InstallSnapshotRpc createInstallSnapshotRpc(int term, int lastIncludedIndex, NodeId selfId, int offset, int length) {
        InstallSnapshotRpc rpc = new InstallSnapshotRpc();
        rpc.setTerm(term);
        rpc.setLeaderId(selfId);
        rpc.setLastIndex(snapshot.getLastIncludedIndex());
        rpc.setLastTerm(snapshot.getLastIncludedTerm());
        if (snapshot.getLastIncludedIndex() != lastIncludedIndex) {
            // snapshot has changed since last create
            rpc.setOffset(0);
        } else {
            rpc.setOffset(offset);
        }
        if (rpc.getOffset() == 0) {
            rpc.setLastConfig(snapshot.getLastConfig());
        }
        SnapshotChunk chunk = snapshot.readData(rpc.getOffset(), length);
        rpc.setData(chunk.toByteArray());
        rpc.setDone(chunk.isLastChunk());
        return rpc;
    }

    @Override
    public Set<NodeEndpoint> getLastGroup() {
        return groupConfigEntryList.getLastGroup();
    }

    @Override
    public GroupConfigEntry getLastGroupConfigEntry() {
        return groupConfigEntryList.getLast();
    }

    @Override
    public int getNextIndex() {
        return entrySequence.getNextLogIndex();
    }

    @Override
    public int getCommitIndex() {
        return commitIndex;
    }

    @Override
    public boolean isNewerThan(int lastLogIndex, int lastLogTerm) {
        EntryMeta lastEntryMeta = getLastEntryMeta();
        logger.debug("last entry ({}, {}), candidate ({}, {})", lastEntryMeta.getIndex(), lastEntryMeta.getTerm(), lastLogIndex, lastLogTerm);
        return lastEntryMeta.getTerm() > lastLogTerm || lastEntryMeta.getIndex() > lastLogIndex;
    }

    @Override
    public NoOpEntry appendEntry(int term) {
        NoOpEntry entry = new NoOpEntry(entrySequence.getNextLogIndex(), term);
        entrySequence.append(entry);
        return entry;
    }

    @Override
    public GeneralEntry appendEntry(int term, byte[] command) {
        GeneralEntry entry = new GeneralEntry(entrySequence.getNextLogIndex(), term, command);
        entrySequence.append(entry);
        return entry;
    }

    @Override
    public AddNodeEntry appendEntryForAddNode(int term, Set<NodeEndpoint> nodeEndpoints, NodeEndpoint newNodeEndpoint) {
        AddNodeEntry entry = new AddNodeEntry(entrySequence.getNextLogIndex(), term, nodeEndpoints, newNodeEndpoint);
        entrySequence.append(entry);
        groupConfigEntryList.add(entry);
        return entry;
    }

    @Override
    public RemoveNodeEntry appendEntryForRemoveNode(int term, Set<NodeEndpoint> nodeEndpoints, NodeId nodeToRemove) {
        RemoveNodeEntry entry = new RemoveNodeEntry(entrySequence.getNextLogIndex(), term, nodeEndpoints, nodeToRemove);
        entrySequence.append(entry);
        groupConfigEntryList.add(entry);
        return entry;
    }

    @Override
    public AppendEntriesState appendEntriesFromLeader(int prevLogIndex, int prevLogTerm, List<Entry> leaderEntries) {
        // check previous log
        if (!checkIfPreviousLogMatches(prevLogIndex, prevLogTerm)) {
            return AppendEntriesState.FAILED;
        }
        // heartbeat
        if (leaderEntries.isEmpty()) {
            return AppendEntriesState.SUCCESS;
        }
        assert prevLogIndex + 1 == leaderEntries.get(0).getIndex();
        UnmatchedLogRemovedResult unmatchedLogRemovedResult = removeUnmatchedLog(new EntrySequenceView(leaderEntries));
        GroupConfigEntry lastGroupConfigEntry = appendEntriesFromLeader(unmatchedLogRemovedResult.newEntries);
        return new AppendEntriesState(
                lastGroupConfigEntry != null ?
                        lastGroupConfigEntry.getResultNodeEndpoints() :
                        unmatchedLogRemovedResult.getGroup()
        );
    }

    private GroupConfigEntry appendEntriesFromLeader(EntrySequenceView leaderEntries) {
        if (leaderEntries.isEmpty()) {
            return null;
        }
        logger.debug("append entries from leader from {} to {}", leaderEntries.getFirstLogIndex(), leaderEntries.getLastLogIndex());
        GroupConfigEntry lastGroupConfigEntry = null;
        for (Entry leaderEntry : leaderEntries) {
            entrySequence.append(leaderEntry);
            if (leaderEntry instanceof GroupConfigEntry) {
                lastGroupConfigEntry = (GroupConfigEntry) leaderEntry;
            }
        }
        return lastGroupConfigEntry;
    }

//    private void appendEntryFromLeader(Entry leaderEntry) {
//        entrySequence.append(leaderEntry);
//        if (leaderEntry instanceof GroupConfigEntry) {
//            eventBus.post(new GroupConfigEntryFromLeaderAppendEvent(
//                    (GroupConfigEntry) leaderEntry)
//            );
//        }
//    }

    private UnmatchedLogRemovedResult removeUnmatchedLog(EntrySequenceView leaderEntries) {
        assert !leaderEntries.isEmpty();
        int firstUnmatched = findFirstUnmatchedLog(leaderEntries);
        if(firstUnmatched < 0) {
            return new UnmatchedLogRemovedResult(EntrySequenceView.EMPTY, null);
        }
        GroupConfigEntry firstRemovedEntry = removeEntriesAfter(firstUnmatched - 1);
        return new UnmatchedLogRemovedResult(leaderEntries.subView(firstUnmatched), firstRemovedEntry);
    }

    private int findFirstUnmatchedLog(EntrySequenceView leaderEntries) {
        assert !leaderEntries.isEmpty();
        int logIndex;
        EntryMeta followerEntryMeta;
        for (Entry leaderEntry : leaderEntries) {
            logIndex = leaderEntry.getIndex();
            followerEntryMeta = entrySequence.getEntryMeta(logIndex);
            if (followerEntryMeta == null || followerEntryMeta.getTerm() != leaderEntry.getTerm()) {
                return logIndex;
            }
        }
        return -1;
    }

    private boolean checkIfPreviousLogMatches(int prevLogIndex, int prevLogTerm) {
        int lastIncludedIndex = snapshot.getLastIncludedIndex();
        if (prevLogIndex < lastIncludedIndex) {
            logger.debug("previous log index {} < snapshot's last included index {}", prevLogIndex, lastIncludedIndex);
            return false;
        }
        if (prevLogIndex == lastIncludedIndex) {
            int lastIncludedTerm = snapshot.getLastIncludedTerm();
            if (prevLogTerm != lastIncludedTerm) {
                logger.debug("previous log index matches snapshot's last included index, " +
                        "but term not (expected {}, actual {})", lastIncludedTerm, prevLogTerm);
                return false;
            }
            return true;
        }
        EntryMeta meta = entrySequence.getEntryMeta(prevLogIndex);
        if (meta == null) {
            logger.debug("previous log {} not found", prevLogIndex);
            return false;
        }
        int term = meta.getTerm();
        if (term != prevLogTerm) {
            logger.debug("different term of previous log, local {}, remote {}", term, prevLogTerm);
            return false;
        }
        return true;
    }

    private GroupConfigEntry removeEntriesAfter(int index) {
        if (entrySequence.isEmpty() || index >= entrySequence.getLastLogIndex()) {
            return null;
        }
        int lastApplied = stateMachine.getLastApplied();
        if (index < lastApplied && entrySequence.subList(index + 1, lastApplied + 1).stream().anyMatch(this::isApplicable)) {
            logger.warn("applied log removed, reapply from start");
            applySnapshot(snapshot);
            logger.debug("apply log from {} to {}", entrySequence.getFirstLogIndex(), index);
            entrySequence.subList(entrySequence.getFirstLogIndex(), index + 1).forEach(this::applyEntry);
        }
        logger.debug("remove entries after {}", index);
        entrySequence.removeAfter(index);
        if (index < commitIndex) {
            commitIndex = index;
        }
        return groupConfigEntryList.removeAfter(index);
//        GroupConfigEntry firstRemovedEntry = groupConfigEntryList.removeAfter(index);
//        if (firstRemovedEntry != null) {
//            logger.info("group config removed");
//            eventBus.post(new GroupConfigEntryBatchRemovedEvent(firstRemovedEntry));
//        }
    }

    @Override
    public List<GroupConfigEntry> advanceCommitIndex(int newCommitIndex, int currentTerm) {
        if (!validateNewCommitIndex(newCommitIndex, currentTerm)) {
            return Collections.emptyList();
        }
        logger.debug("advance commit index from {} to {}", commitIndex, newCommitIndex);
        entrySequence.commit(newCommitIndex);
//        groupConfigsCommitted(newCommitIndex);
        List<GroupConfigEntry> groupConfigEntries = groupConfigEntryList.subList(commitIndex + 1, newCommitIndex + 1);
        logger.debug("group configs from {} to {}, {}", commitIndex + 1, newCommitIndex + 1, groupConfigEntries);
        commitIndex = newCommitIndex;
        advanceApplyIndex();
        return groupConfigEntries;
    }

    @Override
    public void generateSnapshot(int lastIncludedIndex, Set<NodeEndpoint> groupConfig) {
        logger.info("generate snapshot, last included index {}", lastIncludedIndex);
        EntryMeta lastAppliedEntryMeta = entrySequence.getEntryMeta(lastIncludedIndex);
        replaceSnapshot(generateSnapshot(lastAppliedEntryMeta, groupConfig));
    }

    private void advanceApplyIndex() {
        // start up and snapshot exists
        int lastApplied = stateMachine.getLastApplied();
        int lastIncludedIndex = snapshot.getLastIncludedIndex();
        if (lastApplied == 0 && lastIncludedIndex > 0) {
            assert commitIndex >= lastIncludedIndex;
            applySnapshot(snapshot);
            lastApplied = lastIncludedIndex;
        }
        for (Entry entry : entrySequence.subList(lastApplied + 1, commitIndex + 1)) {
            applyEntry(entry);
        }
    }

    private void applySnapshot(Snapshot snapshot) {
        logger.debug("apply snapshot, last included index {}", snapshot.getLastIncludedIndex());
        try {
            stateMachine.applySnapshot(snapshot);
        } catch (IOException e) {
            throw new LogException("failed to apply snapshot", e);
        }
    }

    private void applyEntry(Entry entry) {
        // skip no-op entry and membership-change entry
        if (isApplicable(entry)) {
            Set<NodeEndpoint> lastGroup = groupConfigEntryList.getLastGroupBeforeOrDefault(entry.getIndex());
            stateMachine.applyLog(stateMachineContext, entry.getIndex(), entry.getTerm(), entry.getCommandBytes(), entrySequence.getFirstLogIndex(), lastGroup);
        } else {
            stateMachine.advanceLastApplied(entry.getIndex());
        }
    }

    private boolean isApplicable(Entry entry) {
        return entry.getKind() == Entry.KIND_GENERAL;
    }

//    private void groupConfigsCommitted(int newCommitIndex) {
//        for (GroupConfigEntry groupConfigEntry : groupConfigEntryList.subList(commitIndex + 1, newCommitIndex + 1)) {
//            eventBus.post(new GroupConfigEntryCommittedEvent(groupConfigEntry));
//        }
//    }

    private boolean validateNewCommitIndex(int newCommitIndex, int currentTerm) {
        if (newCommitIndex <= commitIndex) {
            return false;
        }
        Entry entry = entrySequence.getEntry(newCommitIndex);
        if (entry == null) {
            logger.debug("log of new commit index {} not found", newCommitIndex);
            return false;
        }
        if (entry.getTerm() != currentTerm) {
            logger.debug("log term of new commit index != current term ({} != {})", entry.getTerm(), currentTerm);
            return false;
        }
        return true;
    }

    protected abstract Snapshot generateSnapshot(EntryMeta lastAppliedEntryMeta, Set<NodeEndpoint> groupConfig);

    @Override
    public InstallSnapshotState installSnapshot(InstallSnapshotRpc rpc) {
        if (rpc.getLastIndex() <= snapshot.getLastIncludedIndex()) {
            logger.debug("snapshot's last included index from rpc <= current one ({} <= {}), ignore",
                    rpc.getLastIndex(), snapshot.getLastIncludedIndex());
            return new InstallSnapshotState(InstallSnapshotState.StateName.ILLEGAL_INSTALL_SNAPSHOT_RPC);
        }
        if (rpc.getOffset() == 0) {
            assert rpc.getLastConfig() != null;
            snapshotBuilder.close();
            snapshotBuilder = newSnapshotBuilder(rpc);
        } else {
            snapshotBuilder.append(rpc);
        }
        if (!rpc.isDone()) {
            return new InstallSnapshotState(InstallSnapshotState.StateName.INSTALLING);
        }
        Snapshot newSnapshot = snapshotBuilder.build();
        replaceSnapshot(newSnapshot);
        applySnapshot(snapshot);
        int lastIncludedIndex = snapshot.getLastIncludedIndex();
        if (commitIndex < lastIncludedIndex) {
            commitIndex = lastIncludedIndex;
        }
        return new InstallSnapshotState(InstallSnapshotState.StateName.INSTALLED, newSnapshot.getLastConfig());
    }

    protected abstract SnapshotBuilder newSnapshotBuilder(InstallSnapshotRpc firstRpc);

    protected abstract void replaceSnapshot(Snapshot newSnapshot);

    @Override
    public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    @Override
    public StateMachine getStateMachine() {
        return this.stateMachine;
    }

    @Override
    public void close() {
        snapshot.close();
        entrySequence.close();
        snapshotBuilder.close();
        stateMachine.shutdown();
    }

    private static class UnmatchedLogRemovedResult {
        private final EntrySequenceView newEntries;
        private final GroupConfigEntry firstRemovedEntry;

        UnmatchedLogRemovedResult(EntrySequenceView newEntries, GroupConfigEntry firstRemovedEntry) {
            this.newEntries = newEntries;
            this.firstRemovedEntry = firstRemovedEntry;
        }

        Set<NodeEndpoint> getGroup() {
            return firstRemovedEntry != null ? firstRemovedEntry.getNodeEndpoints() : null;
        }
    }

    private static class EntrySequenceView implements Iterable<Entry> {
        static final EntrySequenceView EMPTY = new EntrySequenceView(Collections.emptyList());

        private final List<Entry> entries;
        private int firstLogIndex;
        private int lastLogIndex;

        EntrySequenceView(List<Entry> entries) {
            this.entries = entries;
            if (!entries.isEmpty()) {
                firstLogIndex = entries.get(0).getIndex();
                lastLogIndex = entries.get(entries.size() - 1).getIndex();
            }
        }

        Entry get(int index) {
            if (entries.isEmpty() || index < firstLogIndex || index > lastLogIndex) {
                return null;
            }
            return entries.get(index - firstLogIndex);
        }

        boolean isEmpty() {
            return entries.isEmpty();
        }

        int getFirstLogIndex() {
            return firstLogIndex;
        }

        int getLastLogIndex() {
            return lastLogIndex;
        }

        EntrySequenceView subView(int fromIndex) {
            if (entries.isEmpty() || fromIndex > lastLogIndex) {
                return new EntrySequenceView(Collections.emptyList());
            }
            return new EntrySequenceView(
                    entries.subList(fromIndex - firstLogIndex, entries.size())
            );
        }

        @Override
        @Nonnull
        public Iterator<Entry> iterator() {
            return entries.iterator();
        }

    }

}
