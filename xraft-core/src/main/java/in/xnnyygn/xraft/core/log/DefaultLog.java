package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.entry.*;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryAppendEvent;
import in.xnnyygn.xraft.core.log.snapshot.*;
import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;
import in.xnnyygn.xraft.core.rpc.message.RequestVoteRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class DefaultLog implements Log {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLog.class);
    private static final int INSTALL_SNAPSHOT_RPC_DATA_LENGTH = 10;
    private static final int SNAPSHOT_GENERATE_THRESHOLD = 5;
    private final EventBus eventBus;
    private Snapshot snapshot;
    private EntrySequence entrySequence;
    private GroupConfigEntryList groupConfigEntryList = new GroupConfigEntryList();
    private MemorySnapshotBuilder snapshotBuilder;
    private EntryApplier entryApplier = new NullEntryApplier();
    private SnapshotGenerator snapshotGenerator = new NullSnapshotGenerator();
    private SnapshotApplier snapshotApplier = new NullSnapshotApplier();
    private int commitIndex = 0;
    private int lastApplied = 0;

    public DefaultLog() {
        this(new EventBus());
    }

    public DefaultLog(EventBus eventBus) {
        this(new EmptySnapshot(), new EntrySequence(), eventBus);
    }

    DefaultLog(Snapshot snapshot, EntrySequence entrySequence, EventBus eventBus) {
        this.snapshot = snapshot;
        this.entrySequence = entrySequence;
        this.eventBus = eventBus;
    }

    @Override
    public NoOpEntry appendEntry(int term) {
        return this.entrySequence.append(term);
    }

    @Override
    public GeneralEntry appendEntry(int term, byte[] commandBytes) {
        return this.entrySequence.append(term, commandBytes);
    }

    @Override
    public AddNodeEntry appendEntryForAddNode(int term, Set<NodeConfig> nodeConfigs, NodeConfig newNodeConfig) {
        AddNodeEntry entry = entrySequence.append(term, nodeConfigs, newNodeConfig);
        groupConfigEntryList.add(entry);
        return entry;
    }

    @Override
    public RemoveNodeEntry appendEntryForRemoveNode(int term, Set<NodeConfig> nodeConfigs, NodeId nodeToRemove) {
        RemoveNodeEntry entry = entrySequence.append(term, nodeConfigs, nodeToRemove);
        groupConfigEntryList.add(entry);
        return entry;
    }

    @Override
    public GroupConfigEntry getLastGroupConfigEntry() {
        return this.groupConfigEntryList.getLast();
    }

    @Override
    public int getCommitIndex() {
        return commitIndex;
    }

    @Override
    public boolean appendEntries(AppendEntriesRpc rpc) {
        int prevLogIndex = rpc.getPrevLogIndex();
        if (prevLogIndex > 0) {
            if (prevLogIndex < this.snapshot.getLastIncludedIndex()) {
                logger.debug("previous log index < snapshot's last included index");
                return false;
            }

            if (prevLogIndex == this.snapshot.getLastIncludedIndex()) {
                if (rpc.getPrevLogTerm() != this.snapshot.getLastIncludedTerm()) {
                    logger.debug("previous log term != snapshot's last included term");
                    return false;
                }
            } else {
                assert prevLogIndex > this.snapshot.getLastIncludedIndex();
                Entry prevLog = this.entrySequence.getEntry(prevLogIndex);
                if (prevLog == null || prevLog.getTerm() != rpc.getPrevLogTerm()) {
                    logger.debug("previous log {} not found or different term", prevLogIndex);
                    return false;
                }
            }
        }

        mergeEntries(prevLogIndex + 1, rpc.getEntries());
        // TODO move up
        this.advanceCommitIndex(Math.min(rpc.getLeaderCommit(), rpc.getLastEntryIndex()), rpc.getTerm());
        return true;
    }

    @Override
    public RequestVoteRpc createRequestVoteRpc(int term, NodeId selfNodeId) {
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(term);
        rpc.setCandidateId(selfNodeId);
        if (this.entrySequence.isEmpty()) {
            rpc.setLastLogIndex(this.snapshot.getLastIncludedIndex());
            rpc.setLastLogTerm(this.snapshot.getLastIncludedTerm());
        } else {
            Entry lastEntry = this.entrySequence.getLastEntry();
            rpc.setLastLogIndex(lastEntry.getIndex());
            rpc.setLastLogTerm(lastEntry.getTerm());
        }
        return rpc;
    }

    @Override
    public AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfNodeId, int nextIndex, int maxEntries) {
        if (nextIndex > this.entrySequence.getNextLogIndex()) {
            throw new IllegalArgumentException("illegal next index " + nextIndex);
        }

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setMessageId(UUID.randomUUID().toString());
        rpc.setTerm(term);
        rpc.setLeaderId(selfNodeId);
        rpc.setLeaderCommit(this.commitIndex);

        if (this.entrySequence.isEmpty()) {
            if (nextIndex < this.entrySequence.getNextLogIndex()) {
                throw new EntryInSnapshotException(nextIndex);
            }
            assert nextIndex == this.entrySequence.getNextLogIndex();
            rpc.setPrevLogIndex(this.snapshot.getLastIncludedIndex());
            rpc.setPrevLogTerm(this.snapshot.getLastIncludedTerm());
            return rpc;
        }

        if (nextIndex < this.entrySequence.getFirstLogIndex()) {
            throw new EntryInSnapshotException(nextIndex);
        }
        if (nextIndex == this.entrySequence.getFirstLogIndex()) {
            rpc.setPrevLogIndex(this.snapshot.getLastIncludedIndex());
            rpc.setPrevLogTerm(this.snapshot.getLastIncludedTerm());
        } else {
            Entry entry = this.entrySequence.getEntry(nextIndex - 1);
            assert entry != null;
            rpc.setPrevLogIndex(entry.getIndex());
            rpc.setPrevLogTerm(entry.getTerm());
        }
        rpc.setEntries(this.entrySequence.subList(
                nextIndex,
                (maxEntries < 0 ? this.entrySequence.getNextLogIndex() : Math.min(this.entrySequence.getNextLogIndex(), nextIndex + maxEntries))
        ));
        return rpc;
    }

    @Override
    public InstallSnapshotRpc createInstallSnapshotRpc(int term, NodeId selfNodeId, int offset) {
        InstallSnapshotRpc rpc = new InstallSnapshotRpc();
        rpc.setTerm(term);
        rpc.setLeaderId(selfNodeId);
        rpc.setLastIncludedIndex(snapshot.getLastIncludedIndex());
        rpc.setLastIncludedTerm(snapshot.getLastIncludedTerm());
        rpc.setOffset(offset);

        SnapshotChunk chunk = snapshot.read(offset, INSTALL_SNAPSHOT_RPC_DATA_LENGTH);
        rpc.setData(chunk.toByteArray());
        rpc.setDone(chunk.isLastChunk());
        return rpc;
    }

    @Override
    public void advanceCommitIndex(int newCommitIndex, int currentTerm) {
        if (newCommitIndex <= this.commitIndex) return;

        // newCommitIndex >= this.entrySequence.getFirstLogIndex() && newCommitIndex <= this.entrySequence.getLastLogIndex();
        assert this.entrySequence.getEntry(newCommitIndex) != null;
        if (this.entrySequence.getEntry(newCommitIndex).getTerm() != currentTerm) return;

        logger.debug("advance commit index from {} to {}", this.commitIndex, newCommitIndex);
        // TODO commit items
        for (Entry entry : entrySequence.subList(commitIndex + 1, newCommitIndex + 1)) {

        }
        this.commitIndex = newCommitIndex;

        logger.debug("apply log from {} to {}", this.lastApplied + 1, this.commitIndex);
        for (Entry entry : this.entrySequence.subList(this.lastApplied + 1, this.commitIndex + 1)) {
            List<EntryListener> entryListeners = entry.getListeners();
            entryListeners.forEach(l -> l.entryCommitted(entry));
            this.applyEntry(entry);
            entryListeners.forEach(l -> l.entryApplied(entry));
            entry.removeAllListeners();
        }

        // TODO configurable item: snapshot_generate_threshold, or strategy?
        if (this.lastApplied - this.entrySequence.getFirstLogIndex() >= SNAPSHOT_GENERATE_THRESHOLD) {
            logger.info("generate snapshot, last included index {}", this.lastApplied);
            Entry lastAppliedEntry = this.entrySequence.getEntry(this.lastApplied);
            assert lastAppliedEntry != null;

            Snapshot snapshot = new MemorySnapshot(
                    lastAppliedEntry.getIndex(), lastAppliedEntry.getTerm(), this.snapshotGenerator.generateSnapshot());
            this.replaceSnapshot(snapshot);
        }
    }

    private void applyEntry(Entry entry) {
        // skip no-op entry and membership-change entry
        if (entry instanceof GeneralEntry) {
            this.entryApplier.applyEntry(entry);
        }
        this.lastApplied = entry.getIndex();
    }

    @Override
    public int getNextIndex() {
        return entrySequence.getNextLogIndex();
    }

    @Override
    public boolean isNewerThan(int lastLogIndex, int lastLogTerm) {
        // TODO handle snapshot
        Entry lastEntry = this.entrySequence.getLastEntry();
        if (lastEntry == null) {
            logger.debug("no last entry");
            return false;
        }

        logger.debug("last entry ({}, {}), candidate ({}, {})", lastEntry.getIndex(), lastEntry.getTerm(), lastLogIndex, lastLogTerm);
        return lastEntry.getTerm() > lastLogTerm || lastEntry.getIndex() > lastLogIndex;
    }

    @Override
    public void setEntryApplier(EntryApplier applier) {
        this.entryApplier = applier;
    }

    @Override
    public void installSnapshot(InstallSnapshotRpc rpc) {
        if (rpc.getLastIncludedIndex() <= this.snapshot.getLastIncludedIndex()) {
            logger.debug("snapshot's last included index from rpc <= current one, ignore");
            return;
        }

        if (rpc.getOffset() == 0) {
            this.snapshotBuilder = new MemorySnapshotBuilder(rpc);
        } else {
            if (this.snapshotBuilder == null) {
                throw new IllegalStateException("no snapshot rpc with offset 0");
            }
            this.snapshotBuilder.append(rpc);
        }

        if (!rpc.isDone()) return;

        Snapshot newSnapshot = this.snapshotBuilder.build();
        logger.info("install snapshot, last included index {}", newSnapshot.getLastIncludedIndex());
        // TODO refactor
        this.snapshotApplier.applySnapshot(newSnapshot.toByteArray());
        this.replaceSnapshot(newSnapshot);

        if (this.commitIndex < newSnapshot.getLastIncludedIndex()) {
            this.commitIndex = newSnapshot.getLastIncludedIndex();
        }
        if (this.lastApplied <= newSnapshot.getLastIncludedIndex()) {
            this.lastApplied = newSnapshot.getLastIncludedIndex();
        }
    }

    @Override
    public void setSnapshotGenerator(SnapshotGenerator generator) {
        this.snapshotGenerator = generator;
    }

    @Override
    public void setSnapshotApplier(SnapshotApplier applier) {
        this.snapshotApplier = applier;
    }

    private void replaceSnapshot(Snapshot newSnapshot) {
        logger.debug("replace snapshot with {}", newSnapshot);
        this.snapshot = newSnapshot;
        if (newSnapshot.getLastIncludedIndex() < this.entrySequence.getNextLogIndex()) {
            this.entrySequence.clearBefore(newSnapshot.getLastIncludedIndex() + 1);
        } else {
            this.entrySequence = new EntrySequence(newSnapshot.getLastIncludedIndex() + 1);
        }
        logger.debug("current entry sequence {}", this.entrySequence);
    }

    private void mergeEntries(int fromIndex, List<Entry> leaderEntries) {
        if (leaderEntries.isEmpty()) return;

        int copyFrom = 0;
        if (!this.entrySequence.isEmpty() && fromIndex <= this.entrySequence.getLastLogIndex()) {
            List<Entry> followerEntries = this.entrySequence.subList(fromIndex,
                    Math.min(this.entrySequence.getLastLogIndex() + 1, fromIndex + leaderEntries.size()));
            copyFrom = followerEntries.size();
            for (int i = 0; i < Math.min(followerEntries.size(), leaderEntries.size()); i++) {
                Entry followerEntry = followerEntries.get(i);
                Entry leaderEntry = leaderEntries.get(i);
                if (followerEntry.getTerm() != leaderEntry.getTerm()) {
                    logger.debug("remove entries from {}", followerEntry.getIndex());
                    // TODO rollback group config entry
                    this.entrySequence.clearAfter(followerEntry.getIndex() - 1);
                    copyFrom = i;
                    break;
                }
            }
        }

        if (copyFrom > 0) {
            logger.debug("skip copying {} entries", copyFrom);
        }

        // TODO refactor
        if (copyFrom < leaderEntries.size()) {
            logger.debug("append leader entries from {} to {}", leaderEntries.get(copyFrom).getIndex(), leaderEntries.get(leaderEntries.size() - 1).getIndex());
            List<Entry> entriesToAppend = leaderEntries.subList(copyFrom, leaderEntries.size());

            // append entries to log
            entrySequence.appendEntries(entriesToAppend);

            // filter group config entry
            for (Entry entry : entriesToAppend) {
                if (entry instanceof GroupConfigEntry) {
                    eventBus.post(new GroupConfigEntryAppendEvent((GroupConfigEntry) entry));
                }
            }
        }
    }

}
