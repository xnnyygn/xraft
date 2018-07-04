package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.Entry;

public class NullEntryApplier implements EntryApplier {

    @Override
    public void applyEntry(Entry entry) {
    }

}
