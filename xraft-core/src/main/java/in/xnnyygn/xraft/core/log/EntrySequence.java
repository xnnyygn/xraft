package in.xnnyygn.xraft.core.log;

import java.util.ArrayList;
import java.util.List;

public class EntrySequence {

    private List<Entry> entries = new ArrayList<>();
    private int logIndexOffset;
    private int nextLogIndex;

    public EntrySequence() {
        this(1);
    }

    public EntrySequence(int logIndexOffset) {
        this.logIndexOffset = logIndexOffset;
        this.nextLogIndex = logIndexOffset;
    }

    public void append(int term, byte[] command, EntryAppliedListener listener) {
        this.entries.add(new Entry(this.nextLogIndex++, term, command, listener));
    }

    public void appendEntries(List<Entry> entries) {
        this.entries.addAll(entries);
        this.nextLogIndex += entries.size();
    }

    public int getLastLogIndex() {
        return this.nextLogIndex - 1;
    }

    public List<Entry> subList(int fromIndex, int toIndex) {
        return this.entries.subList(fromIndex - this.logIndexOffset, toIndex - this.logIndexOffset);
    }

    public Entry getEntry(int index) {
        if (index < this.logIndexOffset || index > this.getLastLogIndex()) return null;

        return this.entries.get(index - this.logIndexOffset);
    }

    public Entry getLastEntry() {
        return this.entries.isEmpty() ? null : this.entries.get(this.entries.size() - 1);
    }

    public void clearAfter(int index) {
        if (index > this.getLastLogIndex()) return;

        this.entries.subList(index - this.logIndexOffset, this.entries.size()).clear();
        this.nextLogIndex = index;
    }

    public int getNextLogIndex() {
        return nextLogIndex;
    }

}
