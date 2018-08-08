package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.log.entry.*;
import in.xnnyygn.xraft.core.log.event.GroupConfigEntryAppendEvent;
import in.xnnyygn.xraft.core.log.snapshot.*;
import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.message.AppendEntriesRpc;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

abstract class AbstractLog implements Log {

    private static final int INSTALL_SNAPSHOT_RPC_DATA_LENGTH = 10;
    private static final int SNAPSHOT_GENERATE_THRESHOLD = 5;

    private static final Logger logger = LoggerFactory.getLogger(AbstractLog.class);

    protected final EventBus eventBus;
    protected Snapshot snapshot;
    protected EntrySequence entrySequence;

    protected SnapshotBuilder snapshotBuilder = new NullSnapshotBuilder();
    protected GroupConfigEntryList groupConfigEntryList = new GroupConfigEntryList();
    protected StateMachine stateMachine = new NullStateMachine();
    protected int commitIndex = 0;
    protected int lastApplied = 0;

    AbstractLog(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public EntryMeta getLastEntryMeta() {
        if (entrySequence.isEmpty()) {
            return new EntryMeta(snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm());
        }
        return entrySequence.getLastEntry().getMeta();
    }

    @Override
    public AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfNodeId, int nextIndex, int maxEntries) {
        if (nextIndex > entrySequence.getNextLogIndex()) {
            throw new IllegalArgumentException("illegal next index " + nextIndex);
        }

        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setMessageId(UUID.randomUUID().toString());
        rpc.setTerm(term);
        rpc.setLeaderId(selfNodeId);
        rpc.setLeaderCommit(commitIndex);

        if (entrySequence.isEmpty()) {
            if (nextIndex < entrySequence.getNextLogIndex()) {
                throw new EntryInSnapshotException(nextIndex);
            }

            assert nextIndex == entrySequence.getNextLogIndex();
            rpc.setPrevLogIndex(snapshot.getLastIncludedIndex());
            rpc.setPrevLogTerm(snapshot.getLastIncludedTerm());
            return rpc;
        }

        if (nextIndex < entrySequence.getFirstLogIndex()) {
            throw new EntryInSnapshotException(nextIndex);
        }

        if (nextIndex == entrySequence.getFirstLogIndex()) {
            rpc.setPrevLogIndex(snapshot.getLastIncludedIndex());
            rpc.setPrevLogTerm(snapshot.getLastIncludedTerm());
        } else {
            Entry entry = entrySequence.getEntry(nextIndex - 1);
            assert entry != null;
            rpc.setPrevLogIndex(entry.getIndex());
            rpc.setPrevLogTerm(entry.getTerm());
        }
        int maxIndex = maxEntries < 0 ? entrySequence.getNextLogIndex() : Math.min(entrySequence.getNextLogIndex(), nextIndex + maxEntries);
        rpc.setEntries(entrySequence.subList(nextIndex, maxIndex));
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

        SnapshotChunk chunk = snapshot.readData(offset, INSTALL_SNAPSHOT_RPC_DATA_LENGTH);
        rpc.setData(chunk.toByteArray());
        rpc.setDone(chunk.isLastChunk());
        return rpc;
    }

    @Override
    public GroupConfigEntry getLastGroupConfigEntry() {
        return groupConfigEntryList.getLast();
    }

    @Override
    public int getCommitIndex() {
        return commitIndex;
    }

    @Override
    public int getNextIndex() {
        return entrySequence.getNextLogIndex();
    }

    @Override
    public boolean isNewerThan(int lastLogIndex, int lastLogTerm) {
        EntryMeta lastEntryMeta = getLastEntryMeta();
        logger.debug("last entry ({}, {}), candidate ({}, {})", lastEntryMeta.getIndex(), lastEntryMeta.getTerm(), lastLogIndex, lastLogTerm);
        return lastEntryMeta.getTerm() > lastLogTerm || lastEntryMeta.getIndex() > lastLogIndex;
    }

    @Override
    public NoOpEntry appendEntry(int term) {
        NoOpEntry entry = new NoOpEntry(entrySequence.getAndIncreaseNextLogIndex(), term);
        entrySequence.append(entry);
        return entry;
    }

    @Override
    public GeneralEntry appendEntry(int term, byte[] command) {
        GeneralEntry entry = new GeneralEntry(entrySequence.getAndIncreaseNextLogIndex(), term, command);
        entrySequence.append(entry);
        return entry;
    }

    @Override
    public AddNodeEntry appendEntryForAddNode(int term, Set<NodeConfig> nodeConfigs, NodeConfig newNodeConfig) {
        AddNodeEntry entry = new AddNodeEntry(entrySequence.getAndIncreaseNextLogIndex(), term, nodeConfigs, newNodeConfig);
        entrySequence.append(entry);
        groupConfigEntryList.add(entry);
        return entry;
    }

    @Override
    public RemoveNodeEntry appendEntryForRemoveNode(int term, Set<NodeConfig> nodeConfigs, NodeId nodeToRemove) {
        RemoveNodeEntry entry = new RemoveNodeEntry(entrySequence.getAndIncreaseNextLogIndex(), term, nodeConfigs, nodeToRemove);
        entrySequence.append(entry);
        groupConfigEntryList.add(entry);
        return entry;
    }

    @Override
    public boolean appendEntries(int prevLogIndex, int prevLogTerm, List<Entry> entries) {
        if (prevLogIndex > 0) {
            if (prevLogIndex < snapshot.getLastIncludedIndex()) {
                logger.debug("previous log index < snapshot's last included index");
                return false;
            }

            if (prevLogIndex == snapshot.getLastIncludedIndex()) {
                if (prevLogTerm != snapshot.getLastIncludedTerm()) {
                    logger.debug("previous log term != snapshot's last included term");
                    return false;
                }
            } else {
                assert prevLogIndex > snapshot.getLastIncludedIndex();
                Entry prevLog = entrySequence.getEntry(prevLogIndex);
                if (prevLog == null || prevLog.getTerm() != prevLogTerm) {
                    logger.debug("previous log {} not found or different term", prevLogIndex);
                    return false;
                }
            }
        }

        if (entries.isEmpty()) return true;

        // merge entries
        int fromIndex = prevLogIndex + 1;
        int copyFrom = 0;
        if (!entrySequence.isEmpty() && fromIndex <= entrySequence.getLastLogIndex()) {
            List<Entry> followerEntries = entrySequence.subList(fromIndex,
                    Math.min(entrySequence.getNextLogIndex(), fromIndex + entries.size()));
            copyFrom = followerEntries.size();
            for (int i = 0; i < Math.min(followerEntries.size(), entries.size()); i++) {
                Entry followerEntry = followerEntries.get(i);
                Entry leaderEntry = entries.get(i);
                if (followerEntry.getTerm() != leaderEntry.getTerm()) {
                    logger.debug("remove entries from {}", followerEntry.getIndex());
                    // TODO rollback group config entry
                    entrySequence.removeAfter(followerEntry.getIndex() - 1);
                    copyFrom = i;
                    break;
                }
            }
        }

        if (copyFrom > 0) {
            logger.debug("skip copying {} entries", copyFrom);
        }

        // TODO refactor
        if (copyFrom < entries.size()) {
            logger.debug("append leader entries from {} to {}", entries.get(copyFrom).getIndex(), entries.get(entries.size() - 1).getIndex());
            List<Entry> entriesToAppend = entries.subList(copyFrom, entries.size());

            // append entries to log
            entrySequence.append(entriesToAppend);

            // filter group config entry
            for (Entry entry : entriesToAppend) {
                if (entry instanceof GroupConfigEntry) {
                    eventBus.post(new GroupConfigEntryAppendEvent((GroupConfigEntry) entry));
                }
            }
        }
        return true;
    }

    @Override
    public void advanceCommitIndex(int newCommitIndex, int currentTerm) {
        if (newCommitIndex <= commitIndex) return;
        if (entrySequence.getEntry(newCommitIndex).getTerm() != currentTerm) return;

        logger.debug("advance commit index from {} to {}", commitIndex, newCommitIndex);
        entrySequence.commit(newCommitIndex);
        commitIndex = newCommitIndex;

        if (lastApplied == 0 && snapshot.getLastIncludedIndex() > 0) {
            // start up and snapshot exists
            assert commitIndex >= snapshot.getLastIncludedIndex();
            logger.debug("apply snapshot, last included index {}", snapshot.getLastIncludedIndex());
            try {
                stateMachine.applySnapshot(snapshot.getDataStream());
            } catch (IOException e) {
                throw new SnapshotIOException(e);
            }
            lastApplied = snapshot.getLastIncludedIndex();
        }
        logger.debug("apply log from {} to {}", lastApplied + 1, commitIndex);
        for (Entry entry : entrySequence.subList(lastApplied + 1, commitIndex + 1)) {
            applyEntry(entry);
        }

        if (lastApplied - entrySequence.getFirstLogIndex() >= SNAPSHOT_GENERATE_THRESHOLD) {
            logger.info("generate snapshot, last included index {}", lastApplied);
            Entry lastAppliedEntry = entrySequence.getEntry(lastApplied);
            assert lastAppliedEntry != null;
            replaceSnapshot(generateSnapshot(lastAppliedEntry));
        }
    }

    protected abstract Snapshot generateSnapshot(Entry lastAppliedEntry);

    private void applyEntry(Entry entry) {
        List<EntryListener> entryListeners = entry.getListeners();
        entryListeners.forEach(l -> l.entryCommitted(entry));
        // skip no-op entry and membership-change entry
        if (entry instanceof GeneralEntry) {
            stateMachine.applyLog(entry.getIndex(), entry.getCommandBytes());
        }
        entryListeners.forEach(l -> l.entryApplied(entry));
        entry.removeAllListeners();
        lastApplied = entry.getIndex();
    }

    @Override
    public void installSnapshot(InstallSnapshotRpc rpc) {
        if (rpc.getLastIncludedIndex() <= snapshot.getLastIncludedIndex()) {
            logger.debug("snapshot's last included index from rpc <= current one, ignore");
            return;
        }
        if (rpc.getOffset() == 0) {
            snapshotBuilder.close();
            snapshotBuilder = newSnapshotBuilder(rpc);
        } else {
            snapshotBuilder.append(rpc);
        }

        if (!rpc.isDone())
            return;

        Snapshot newSnapshot = snapshotBuilder.build();
        logger.info("install snapshot, last included index {}", newSnapshot.getLastIncludedIndex());
        try {
            stateMachine.applySnapshot(newSnapshot.getDataStream());
        } catch (IOException e) {
            throw new SnapshotIOException(e);
        }
        replaceSnapshot(newSnapshot);
        if (commitIndex < newSnapshot.getLastIncludedIndex()) {
            commitIndex = newSnapshot.getLastIncludedIndex();
        }
        if (lastApplied <= newSnapshot.getLastIncludedIndex()) {
            lastApplied = newSnapshot.getLastIncludedIndex();
        }
    }

    protected abstract SnapshotBuilder newSnapshotBuilder(InstallSnapshotRpc firstRpc);

    protected abstract void replaceSnapshot(Snapshot newSnapshot);

    @Override
    public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    @Override
    public void close() {
        snapshot.close();
        entrySequence.close();
        snapshotBuilder.close();
    }

}
