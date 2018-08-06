package in.xnnyygn.xraft.core.log.entry;

import in.xnnyygn.xraft.core.node.NodeConfig;
import in.xnnyygn.xraft.core.node.NodeId;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class EntrySequence {

    private final List<Entry> entries = new ArrayList<>();
    private int logIndexOffset;
    private int nextLogIndex;

    public EntrySequence() {
        this(1);
    }

    public EntrySequence(int logIndexOffset) {
        this.logIndexOffset = logIndexOffset;
        this.nextLogIndex = logIndexOffset;
    }

    public NoOpEntry append(int term) {
        NoOpEntry entry = new NoOpEntry(this.nextLogIndex++, term);
        this.entries.add(entry);
        return entry;
    }

    public GeneralEntry append(int term, byte[] commandBytes) {
        // TODO move index to outside
        GeneralEntry entry = new GeneralEntry(this.nextLogIndex++, term, commandBytes);
        this.entries.add(entry);
        return entry;
    }

    public AddNodeEntry append(int term, Set<NodeConfig> nodeConfigs, NodeConfig newNodeConfig) {
        AddNodeEntry entry = new AddNodeEntry(nextLogIndex++, term, nodeConfigs, newNodeConfig);
        entries.add(entry);
        return entry;
    }

    public RemoveNodeEntry append(int term, Set<NodeConfig> nodeConfigs, NodeId nodeToRemove) {
        RemoveNodeEntry entry = new RemoveNodeEntry(nextLogIndex++, term, nodeConfigs, nodeToRemove);
        entries.add(entry);
        return entry;
    }

    public void appendEntries(List<Entry> entries) {
        this.entries.addAll(entries);
        this.nextLogIndex += entries.size();
    }

    public boolean isEmpty() {
        return this.entries.isEmpty();
    }

    public int getFirstLogIndex() {
        if (this.isEmpty()) throw new EmptySequenceException();

        return doGetFirstLogIndex();
    }

    private int doGetFirstLogIndex() {
        return this.logIndexOffset;
    }

    public int getLastLogIndex() {
        if (this.isEmpty()) throw new EmptySequenceException();

        return doGetLastLogIndex();
    }

    private int doGetLastLogIndex() {
        return this.nextLogIndex - 1;
    }

    // [fromIndex, toIndex)
    public List<Entry> subList(int fromIndex, int toIndex) {
        if (this.isEmpty()) {
            throw new IllegalStateException("sequence is empty");
        }
        if (fromIndex < this.doGetFirstLogIndex() || toIndex > this.doGetLastLogIndex() + 1 || fromIndex > toIndex) {
            throw new IllegalArgumentException("illegal from index " + fromIndex + " or to index " + toIndex);
        }
        return new ArrayList<>(this.entries.subList(fromIndex - this.logIndexOffset, toIndex - this.logIndexOffset));
    }

    public Entry getEntry(int index) {
        if (index < this.doGetFirstLogIndex() || index > this.doGetLastLogIndex()) return null;

        return this.entries.get(index - this.logIndexOffset);
    }

    public Entry getLastEntry() {
        return this.isEmpty() ? null : this.entries.get(this.entries.size() - 1);
    }

    public void clearAfter(int index) {
        if (this.isEmpty()) {
            throw new IllegalStateException("empty sequence");
        }

        if (index >= this.doGetLastLogIndex()) return;

        if (index < this.doGetFirstLogIndex()) {
            this.entries.clear();
            this.nextLogIndex = this.logIndexOffset;
        } else {
            this.entries.subList(index - this.logIndexOffset + 1, this.entries.size()).clear();
            this.nextLogIndex = index + 1;
        }
    }

    public void clearBefore(int index) {
        if (index <= this.doGetFirstLogIndex()) return;

        if (index > this.doGetLastLogIndex()) {
            this.entries.clear();
            this.logIndexOffset = this.nextLogIndex;
        } else {
            this.entries.subList(0, index - this.logIndexOffset).clear();
            this.logIndexOffset = index;
        }
    }

    public int getNextLogIndex() {
        return this.nextLogIndex;
    }

    @Override
    public String toString() {
        return "EntrySequence{" +
                "entries.size=" + entries.size() +
                ", logIndexOffset=" + logIndexOffset +
                ", nextLogIndex=" + nextLogIndex +
                '}';
    }

}
