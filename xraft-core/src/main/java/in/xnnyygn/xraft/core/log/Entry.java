package in.xnnyygn.xraft.core.log;

public class Entry {

    private final int index;
    private final int term;
    private final byte[] command;
    private EntryApplier applier;

    public Entry(int index, int term, byte[] command) {
        this(index, term, command, null);
    }

    public Entry(int index, int term, byte[] command, EntryApplier applier) {
        this.index = index;
        this.term = term;
        this.command = command;
        this.applier = applier;
    }

    public int getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }

    public byte[] getCommand() {
        return command;
    }

    public void apply(EntryApplier fallback) {
        assert fallback != null;
        EntryApplier applier = (this.applier != null ? this.applier : fallback);
        applier.applyEntry(this);
    }

    public Entry copy() {
        return new Entry(this.index, this.term, this.command);
    }

    @Override
    public String toString() {
        return "Entry{" +
                "index=" + index +
                ", term=" + term +
                '}';
    }

}
