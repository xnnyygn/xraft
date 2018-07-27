package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.Entry;
import in.xnnyygn.xraft.core.log.entry.EntryApplier;

import java.util.ArrayList;
import java.util.List;

class EntryApplyRecorder implements EntryApplier {

    private final List<Entry> entries = new ArrayList<>();

    @Override
    public void applyEntry(Entry entry) {
        this.entries.add(entry);
    }

    public List<Entry> getEntries() {
        return entries;
    }

}
