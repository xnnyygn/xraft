package in.xnnyygn.xraft.core.log;

import com.google.common.eventbus.EventBus;
import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.rpc.AppendEntriesRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class MemoryLog implements Log {

    private static Logger logger = LoggerFactory.getLogger(MemoryLog.class);
    private final EventBus eventBus;

    private EntrySequence entrySequence = new EntrySequence();
    private int commitIndex = 0;
    private int lastApplied = 0;

    public MemoryLog(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public void appendEntry(int term, byte[] command) {
        this.appendEntry(term, command, null);
    }

    @Override
    public void appendEntry(int term, byte[] command, EntryAppliedListener listener) {
        logger.info("append entry, term {}", term);
        this.entrySequence.append(term, command, listener);
    }

    @Override
    public boolean appendEntries(AppendEntriesRpc rpc) {
        if (rpc.getPrevLogIndex() > 0) {
            Entry prevLog = this.entrySequence.getEntry(rpc.getPrevLogIndex());
            if (prevLog == null || prevLog.getTerm() != rpc.getPrevLogTerm()) return false;
        }

        mergeEntries(rpc.getPrevLogIndex() + 1, rpc.getEntries());
        this.advanceCommitIndexIfAvailable(Math.min(rpc.getLeaderCommit(), rpc.getLastEntryIndex()));
        return true;
    }

    @Override
    public AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfNodeId, int nextIndex, int maxEntries) {
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(term);
        rpc.setLeaderId(selfNodeId);
        Entry entry = this.entrySequence.getEntry(nextIndex - 1);
        if (entry != null) {
            rpc.setPrevLogIndex(entry.getIndex());
            rpc.setPrevLogTerm(entry.getTerm());
        }
        rpc.setEntries(this.entrySequence.subList(nextIndex,
                (maxEntries < 0 ? this.getLastLogIndex() + 1 : Math.min(this.getLastLogIndex() + 1, nextIndex + maxEntries))
        ).stream().map(Entry::copy).collect(Collectors.toList()));
        rpc.setLeaderCommit(this.commitIndex);
        return rpc;
    }

    @Override
    public void advanceCommitIndexIfAvailable(int newCommitIndex) {
        if (newCommitIndex > this.commitIndex) { // newCommitIndex <= this.entrySequence.getLastLogIndex()
            logger.debug("advance commit index from {} to {}", this.commitIndex, newCommitIndex);
            this.commitIndex = newCommitIndex;

            logger.debug("apply log from {} to {}", this.lastApplied + 1, this.commitIndex);
            for (Entry entry : this.entrySequence.subList(this.lastApplied + 1, this.commitIndex + 1)) {
                eventBus.post(new ApplyEntryMessage(entry));
                entry.notifyApplied();
                this.lastApplied = entry.getIndex();
            }
        }
    }

    @Override
    public int getLastLogIndex() {
        return this.entrySequence.getLastLogIndex();
    }

    @Override
    public boolean isNewerThan(int lastLogIndex, int lastLogTerm) {
        Entry lastEntry = this.entrySequence.getLastEntry();
        if (lastEntry == null) return false;

        return lastEntry.getTerm() > lastLogTerm || lastEntry.getIndex() > lastLogIndex;
    }

    private void mergeEntries(int fromIndex, List<Entry> leaderEntries) {
        if (leaderEntries.isEmpty()) return;

        List<Entry> followerEntries = this.entrySequence.subList(fromIndex,
                Math.min(this.entrySequence.getLastLogIndex() + 1, fromIndex + leaderEntries.size()));
        int copyFrom = followerEntries.size();
        for (int i = 0; i < Math.min(followerEntries.size(), leaderEntries.size()); i++) {
            Entry followerEntry = followerEntries.get(i);
            Entry leaderEntry = leaderEntries.get(i);
            if (followerEntry.getTerm() != leaderEntry.getTerm()) {
                logger.debug("remove entries from {}", followerEntry.getIndex());
                this.entrySequence.clearAfter(followerEntry.getIndex());
                copyFrom = i;
                break;
            }
        }

        if (copyFrom > 0) {
            logger.debug("skip copying {} entries", copyFrom);
        }
        if (copyFrom < leaderEntries.size()) {
            logger.debug("append leader entries from {} to {}", leaderEntries.get(copyFrom).getIndex(), leaderEntries.get(leaderEntries.size() - 1).getIndex());
            this.entrySequence.appendEntries(leaderEntries.subList(copyFrom, leaderEntries.size()));
        }
    }

}
