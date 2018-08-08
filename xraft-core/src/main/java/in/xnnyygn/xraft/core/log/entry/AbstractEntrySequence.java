package in.xnnyygn.xraft.core.log.entry;

import java.util.List;

abstract class AbstractEntrySequence implements EntrySequence {

    protected int logIndexOffset;
    protected int nextLogIndex;

    AbstractEntrySequence(int logIndexOffset) {
        this.logIndexOffset = logIndexOffset;
        this.nextLogIndex = logIndexOffset;
    }

    @Override
    public void append(List<Entry> entries) {
        for (Entry entry : entries) {
            append(entry);
        }
    }

    @Override
    public boolean isEmpty() {
        return logIndexOffset == nextLogIndex;
    }

    @Override
    public int getFirstLogIndex() {
        if (isEmpty()) throw new EmptySequenceException();

        return doGetFirstLogIndex();
    }

    protected int doGetFirstLogIndex() {
        return logIndexOffset;
    }

    @Override
    public int getLastLogIndex() {
        if (isEmpty()) throw new EmptySequenceException();

        return doGetLastLogIndex();
    }

    protected int doGetLastLogIndex() {
        return nextLogIndex - 1;
    }

    @Override
    public Entry getEntry(int index) {
        if (index < doGetFirstLogIndex() || index > doGetLastLogIndex())
            return null;
        return doGetEntry(index);
    }

    protected abstract Entry doGetEntry(int index);

    @Override
    public Entry getLastEntry() {
        return isEmpty() ? null : doGetEntry(doGetLastLogIndex());
    }

    @Override
    public List<Entry> subList(int fromIndex) {
        return subList(fromIndex, nextLogIndex);
    }

    // [fromIndex, toIndex)
    @Override
    public List<Entry> subList(int fromIndex, int toIndex) {
        if (isEmpty()) {
            throw new IllegalStateException("sequence is empty");
        }
        if (fromIndex < doGetFirstLogIndex() || toIndex > doGetLastLogIndex() + 1 || fromIndex > toIndex) {
            throw new IllegalArgumentException("illegal from index " + fromIndex + " or to index " + toIndex);
        }
        return doSubList(fromIndex, toIndex);
    }

    protected abstract List<Entry> doSubList(int fromIndex, int toIndex);

    @Override
    public int getNextLogIndex() {
        return nextLogIndex;
    }

    @Override
    public int getAndIncreaseNextLogIndex() {
        return nextLogIndex++;
    }

}
