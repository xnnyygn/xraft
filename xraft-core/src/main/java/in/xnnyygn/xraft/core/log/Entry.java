package in.xnnyygn.xraft.core.log;

import java.util.Collections;
import java.util.List;

public class Entry {

    private final int index;
    private final int term;
    private final byte[] command;
    private List<EntryAppliedListener> entryAppliedListeners;

    public Entry(int index, int term, byte[] command) {
        this(index, term, command, null);
    }

    public Entry(int index, int term, byte[] command, EntryAppliedListener listener) {
        this.index = index;
        this.term = term;
        this.command = command;
        this.entryAppliedListeners = listener != null ? Collections.singletonList(listener) : Collections.emptyList();
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

    public void notifyApplied() {
        for (EntryAppliedListener listener : this.entryAppliedListeners) {
            listener.entryApplied(this);
        }
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
