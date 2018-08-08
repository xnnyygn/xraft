package in.xnnyygn.xraft.core.log.entry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

abstract class AbstractEntry implements Entry {

    private final int kind;
    protected final int index;
    protected final int term;
    private final List<EntryListener> listeners = new ArrayList<>();

    AbstractEntry(int kind, int index, int term) {
        this.kind = kind;
        this.index = index;
        this.term = term;
    }

    @Override
    public int getKind() {
        return this.kind;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public EntryMeta getMeta() {
        return new EntryMeta(index, term);
    }

    @Override
    public List<EntryListener> getListeners() {
        return Collections.unmodifiableList(listeners);
    }

    @Override
    public void removeAllListeners() {
        this.listeners.clear();
    }

    public void addListener(EntryListener listener) {
        this.listeners.add(listener);
    }

}
