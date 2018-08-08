package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.log.LogDir;
import in.xnnyygn.xraft.core.log.LogException;
import in.xnnyygn.xraft.core.support.RandomAccessFileAdapter;
import in.xnnyygn.xraft.core.support.SeekableFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class FileEntrySequence extends AbstractEntrySequence {

    private static final long OFFSET_MAX_ENTRY_INDEX = Integer.BYTES;
    private final EntryFactory entryFactory = new EntryFactory();

    private final SeekableFile entriesFile;
    private final SeekableFile entryOffsetIndexFile;
    private final Map<Integer, Long> entryOffsetMap = new HashMap<>();
    private final LinkedList<Entry> pendingEntries = new LinkedList<>();
    private int lastEntryIndexInFile = 0;

    public FileEntrySequence(LogDir logDir, int logIndexOffset) {
        super(logIndexOffset);
        try {
            this.entriesFile = new RandomAccessFileAdapter(logDir.getEntriesFile());
            this.entryOffsetIndexFile = new RandomAccessFileAdapter(logDir.getEntryOffsetIndexFile());
        } catch (FileNotFoundException e) {
            throw new LogException(e);
        }
        lastEntryIndexInFile = logIndexOffset - 1;
    }

    public FileEntrySequence(SeekableFile entriesFile, SeekableFile entryOffsetIndexFile, int logIndexOffset) {
        super(logIndexOffset);
        this.entriesFile = entriesFile;
        this.entryOffsetIndexFile = entryOffsetIndexFile;
        lastEntryIndexInFile = logIndexOffset - 1;
    }

    public void load() {
        int minEntryIndex;
        int maxEntryIndex;
        try {
            if (entryOffsetIndexFile.size() == 0)
                return;

            minEntryIndex = entryOffsetIndexFile.readInt();
            maxEntryIndex = entryOffsetIndexFile.readInt();
            for (int i = minEntryIndex; i <= maxEntryIndex; i++) {
                entryOffsetMap.put(i, entryOffsetIndexFile.readLong());
            }
        } catch (IOException e) {
            throw new LogException(e);
        }
        logIndexOffset = minEntryIndex;
        nextLogIndex = maxEntryIndex + 1;
        lastEntryIndexInFile = maxEntryIndex;
    }

    @Override
    protected List<Entry> doSubList(int fromIndex, int toIndex) {
        List<Entry> result = new ArrayList<>();
        if (fromIndex <= lastEntryIndexInFile) {
            for (int i = fromIndex; i < Math.min(lastEntryIndexInFile + 1, toIndex); i++) {
                result.add(getEntryInFile(i));
            }
        }
        if (toIndex > lastEntryIndexInFile + 1) {
            Iterator<Entry> iterator = pendingEntries.iterator();
            int i = lastEntryIndexInFile + 1;
            Entry entry;
            while (iterator.hasNext()) {
                if (i >= toIndex)
                    break;
                entry = iterator.next();
                if (i >= fromIndex)
                    result.add(entry);
                i++;
            }
        }
        return result;
    }

    @Override
    protected Entry doGetEntry(int index) {
        if (index > lastEntryIndexInFile) {
            return pendingEntries.get(index - lastEntryIndexInFile - 1);
        }
        return getEntryInFile(index);
    }

    private Entry getEntryInFile(int index) {
        // TODO add cache
        try {
            entriesFile.seek(getOffsetOfEntry(index));
            return readEntry();
        } catch (IOException e) {
            throw new LogException(e);
        }
    }

    @Override
    public Entry getLastEntry() {
        if (isEmpty())
            return null;
        if (!pendingEntries.isEmpty())
            return pendingEntries.getLast();
        return getEntryInFile(lastEntryIndexInFile);
    }

    private long getOffsetOfEntry(int index) {
        Long offset = entryOffsetMap.get(index);
        if (offset == null) {
            throw new IllegalStateException("no offset for entry " + index);
        }
        return offset;
    }

    private Entry readEntry() throws IOException {
        int kind = entriesFile.readInt();
        int index = entriesFile.readInt();
        int term = entriesFile.readInt();
        int commandBytesLength = entriesFile.readInt();
        byte[] commandBytes = new byte[commandBytesLength];
        entriesFile.read(commandBytes);
        return entryFactory.create(kind, index, term, commandBytes);
    }

    @Override
    public void append(Entry entry) {
        pendingEntries.add(entry);
    }

    @Override
    public void append(List<Entry> entries) {
        pendingEntries.addAll(entries);
    }

    @Override
    public void commit(int index) {
        if (index < lastEntryIndexInFile) {
            throw new IllegalArgumentException("commit index < " + lastEntryIndexInFile);
        }

        if (index == lastEntryIndexInFile)
            return;

        if (pendingEntries.isEmpty() || pendingEntries.getLast().getIndex() < index) {
            throw new IllegalArgumentException("no entry to commit or commit index exceed");
        }

        long offset;
        for (int i = lastEntryIndexInFile + 1; i <= index; i++) {
            try {
                offset = appendEntry(pendingEntries.getFirst());
            } catch (IOException e) {
                throw new LogException(e);
            }
            pendingEntries.removeFirst();
            entryOffsetMap.put(i, offset);
            lastEntryIndexInFile = i;
        }

        // flush
        try {
            entriesFile.flush();
            entryOffsetIndexFile.flush();
        } catch (IOException e) {
            throw new LogException(e);
        }
    }

    private long appendEntry(Entry entry) throws IOException {
        long offset = entriesFile.size();
        entriesFile.seek(offset);
        writeEntry(entry);
        writeEntryOffset(entry.getIndex(), offset);
        return offset;
    }

    private void writeEntry(Entry entry) throws IOException {
        entriesFile.writeInt(entry.getKind());
        entriesFile.writeInt(entry.getIndex());
        entriesFile.writeInt(entry.getTerm());
        byte[] commandBytes = entry.getCommandBytes();
        entriesFile.writeInt(commandBytes.length);
        entriesFile.write(commandBytes);
    }

    private void writeEntryOffset(int index, long offset) throws IOException {
        if (entryOffsetIndexFile.size() == 0L) {
            entryOffsetIndexFile.writeInt(index);
        } else {
            entryOffsetIndexFile.seek(OFFSET_MAX_ENTRY_INDEX); // skip min entry index
        }
        // write max entry index
        entryOffsetIndexFile.writeInt(index);
        // move to position after last entry offset
        entryOffsetIndexFile.seek(getOffsetOfEntryOffsetIndex(index));
        entryOffsetIndexFile.writeLong(offset);
    }

    @Override
    public void removeAfter(int index) {
        if (isEmpty()) {
            throw new IllegalStateException("empty sequence");
        }

        if (index >= doGetLastLogIndex())
            return;

        if (index >= lastEntryIndexInFile) {
            // remove last n entries
            for (int i = pendingEntries.size() + lastEntryIndexInFile - index; i > 0; i--) {
                pendingEntries.removeLast();
            }
            nextLogIndex = index + 1;
            return;
        }

        try {
            if (index < doGetFirstLogIndex()) {
                pendingEntries.clear();
                entriesFile.truncate(0L);
                entryOffsetIndexFile.truncate(0L);
                nextLogIndex = logIndexOffset;
            } else {
                // index >= firstLogIndex && index <= lastEntryIndexInFile
                pendingEntries.clear();
                entriesFile.truncate(getOffsetOfEntry(index + 1));
                entryOffsetIndexFile.seek(OFFSET_MAX_ENTRY_INDEX);
                entryOffsetIndexFile.writeInt(index);
                entryOffsetIndexFile.truncate(getOffsetOfEntryOffsetIndex(index + 1));
                for (int i = index + 1; i < lastEntryIndexInFile; i++) {
                    entryOffsetMap.remove(i);
                }
                lastEntryIndexInFile = index;
                nextLogIndex = index + 1;
            }
        } catch (IOException e) {
            throw new LogException(e);
        }
    }

    private long getOffsetOfEntryOffsetIndex(int index) {
        return (index - logIndexOffset) * Long.BYTES + Integer.BYTES * 2;
    }

    public int getLastEntryIndexInFile() {
        return lastEntryIndexInFile;
    }

    @Override
    public void close() {
        try {
            entriesFile.close();
            entryOffsetIndexFile.close();
        } catch (IOException e) {
            throw new LogException("failed to close", e);
        }
    }

}
