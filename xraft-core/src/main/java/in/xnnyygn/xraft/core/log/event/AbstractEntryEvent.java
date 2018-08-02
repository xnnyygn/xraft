package in.xnnyygn.xraft.core.log.event;

import in.xnnyygn.xraft.core.log.entry.Entry;

class AbstractEntryEvent {

    protected final Entry entry;

    AbstractEntryEvent(Entry entry) {
        this.entry = entry;
    }

    public Entry getEntry() {
        return entry;
    }

}
