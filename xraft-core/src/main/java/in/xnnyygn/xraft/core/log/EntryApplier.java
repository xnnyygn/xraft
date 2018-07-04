package in.xnnyygn.xraft.core.log;

import in.xnnyygn.xraft.core.log.entry.Entry;

public interface EntryApplier {

    void applyEntry(Entry entry);

}
