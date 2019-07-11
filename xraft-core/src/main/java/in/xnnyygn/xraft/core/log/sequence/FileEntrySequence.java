package in.xnnyygn.xraft.core.log.sequence;

import in.xnnyygn.xraft.core.log.LogDir;
import in.xnnyygn.xraft.core.log.LogException;
import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.EntryFactory;
import in.xnnyygn.xraft.core.log.entry.EntryMeta;
import in.xnnyygn.xraft.core.log.entry.GroupConfigEntry;
import in.xnnyygn.xraft.core.node.NodeEndpoint;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.*;

@NotThreadSafe
public class FileEntrySequence extends AbstractEntrySequence {

    private final EntryFactory entryFactory = new EntryFactory();
    private final EntriesFile entriesFile;
    private final EntryIndexFile entryIndexFile;
    private final LinkedList<Entry> pendingEntries = new LinkedList<>();
    private int commitIndex;

    public FileEntrySequence(LogDir logDir, int logIndexOffset) {
        super(logIndexOffset);
        try {
            this.entriesFile = new EntriesFile(logDir.getEntriesFile());
            this.entryIndexFile = new EntryIndexFile(logDir.getEntryOffsetIndexFile());
            initialize();
        } catch (IOException e) {
            throw new LogException("failed to open entries file or entry index file", e);
        }
    }

    public FileEntrySequence(EntriesFile entriesFile, EntryIndexFile entryIndexFile, int logIndexOffset) {
        super(logIndexOffset);
        this.entriesFile = entriesFile;
        this.entryIndexFile = entryIndexFile;
        initialize();
    }

    private void initialize() {
        if (entryIndexFile.isEmpty()) {
            commitIndex = logIndexOffset - 1;
            return;
        }
        logIndexOffset = entryIndexFile.getMinEntryIndex();
        nextLogIndex = entryIndexFile.getMaxEntryIndex() + 1;
        commitIndex = entryIndexFile.getMaxEntryIndex();
    }

    @Override
    public int getCommitIndex() {
        return commitIndex;
    }

    @Override
    public GroupConfigEntryList buildGroupConfigEntryList(Set<NodeEndpoint> initialGroup) {
        GroupConfigEntryList list = new GroupConfigEntryList(initialGroup);

        // check file
        try {
            int entryKind;
            for (EntryIndexItem indexItem : entryIndexFile) {
                entryKind = indexItem.getKind();
                if (entryKind == Entry.KIND_ADD_NODE || entryKind == Entry.KIND_REMOVE_NODE) {
                    list.add((GroupConfigEntry) entriesFile.loadEntry(indexItem.getOffset(), entryFactory));
                }
            }
        } catch (IOException e) {
            throw new LogException("failed to load entry", e);
        }

        // check pending entries
        for (Entry entry : pendingEntries) {
            if (entry instanceof GroupConfigEntry) {
                list.add((GroupConfigEntry) entry);
            }
        }
        return list;
    }

    @Override
    protected List<Entry> doSubList(int fromIndex, int toIndex) {
        List<Entry> result = new ArrayList<>();

        // entries from file
        if (!entryIndexFile.isEmpty() && fromIndex <= entryIndexFile.getMaxEntryIndex()) {
            int maxIndex = Math.min(entryIndexFile.getMaxEntryIndex() + 1, toIndex);
            for (int i = fromIndex; i < maxIndex; i++) {
                result.add(getEntryInFile(i));
            }
        }

        // entries from pending entries
        if (!pendingEntries.isEmpty() && toIndex > pendingEntries.getFirst().getIndex()) {
            Iterator<Entry> iterator = pendingEntries.iterator();
            Entry entry;
            int index;
            while (iterator.hasNext()) {
                entry = iterator.next();
                index = entry.getIndex();
                if (index >= toIndex) {
                    break;
                }
                if (index >= fromIndex) {
                    result.add(entry);
                }
            }
        }
        return result;
    }

    @Override
    protected Entry doGetEntry(int index) {
        if (!pendingEntries.isEmpty()) {
            int firstPendingEntryIndex = pendingEntries.getFirst().getIndex();
            if (index >= firstPendingEntryIndex) {
                return pendingEntries.get(index - firstPendingEntryIndex);
            }
        }

        // pending entries not empty but index < firstPendingEntryIndex => entry in file
        // pending entries empty => entry in file
        assert !entryIndexFile.isEmpty();
        return getEntryInFile(index);
    }

    @Override
    public EntryMeta getEntryMeta(int index) {
        if (!isEntryPresent(index)) {
            return null;
        }
        if (!pendingEntries.isEmpty()) {
            int firstPendingEntryIndex = pendingEntries.getFirst().getIndex();
            if (index >= firstPendingEntryIndex) {
                return pendingEntries.get(index - firstPendingEntryIndex).getMeta();
            }
        }
        return entryIndexFile.get(index).toEntryMeta();
    }

    private Entry getEntryInFile(int index) {
        long offset = entryIndexFile.getOffset(index);
        try {
            return entriesFile.loadEntry(offset, entryFactory);
        } catch (IOException e) {
            throw new LogException("failed to load entry " + index, e);
        }
    }

    @Override
    public Entry getLastEntry() {
        if (isEmpty()) {
            return null;
        }
        if (!pendingEntries.isEmpty()) {
            return pendingEntries.getLast();
        }
        assert !entryIndexFile.isEmpty();
        return getEntryInFile(entryIndexFile.getMaxEntryIndex());
    }

    @Override
    protected void doAppend(Entry entry) {
        pendingEntries.add(entry);
    }

    @Override
    public void commit(int index) {
        if (index < commitIndex) {
            throw new IllegalArgumentException("commit index < " + commitIndex);
        }
        if (index == commitIndex) {
            return;
        }
        if (!entryIndexFile.isEmpty() && index <= entryIndexFile.getMaxEntryIndex()) {
            commitIndex = index;
            return;
        }
        if (pendingEntries.isEmpty() ||
                pendingEntries.getFirst().getIndex() > index ||
                pendingEntries.getLast().getIndex() < index) {
            throw new IllegalArgumentException("no entry to commit or commit index exceed");
        }
        long offset;
        Entry entry = null;
        try {
            for (int i = pendingEntries.getFirst().getIndex(); i <= index; i++) {
                entry = pendingEntries.removeFirst();
                offset = entriesFile.appendEntry(entry);
                entryIndexFile.appendEntryIndex(i, offset, entry.getKind(), entry.getTerm());
                commitIndex = i;
            }
        } catch (IOException e) {
            throw new LogException("failed to commit entry " + entry, e);
        }
    }

    @Override
    protected void doRemoveAfter(int index) {
        if (!pendingEntries.isEmpty() && index >= pendingEntries.getFirst().getIndex() - 1) {
            // remove last n entries in pending entries
            for (int i = index + 1; i <= doGetLastLogIndex(); i++) {
                pendingEntries.removeLast();
            }
            nextLogIndex = index + 1;
            return;
        }
        try {
            if (index >= doGetFirstLogIndex()) {
                pendingEntries.clear();
                // remove entries whose index >= (index + 1)
                entriesFile.truncate(entryIndexFile.getOffset(index + 1));
                entryIndexFile.removeAfter(index);
                nextLogIndex = index + 1;
                commitIndex = index;
            } else {
                pendingEntries.clear();
                entriesFile.clear();
                entryIndexFile.clear();
                nextLogIndex = logIndexOffset;
                commitIndex = logIndexOffset - 1;
            }
        } catch (IOException e) {
            throw new LogException(e);
        }
    }

    @Override
    public void close() {
        try {
            entriesFile.close();
            entryIndexFile.close();
        } catch (IOException e) {
            throw new LogException("failed to close", e);
        }
    }

}
