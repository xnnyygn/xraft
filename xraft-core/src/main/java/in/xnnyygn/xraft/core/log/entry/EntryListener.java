package in.xnnyygn.xraft.core.log.entry;

public interface EntryListener {

    void entryCommitted(Entry entry);

    void entryApplied(Entry entry);

}
