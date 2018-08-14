package in.xnnyygn.xraft.core.log.event;

import in.xnnyygn.xraft.core.log.entry.Entry;

abstract class AbstractEntryEvent<T extends Entry> {

    protected final T entry;

    AbstractEntryEvent(T entry) {
        this.entry = entry;
    }

    public T getEntry() {
        return entry;
    }

}
