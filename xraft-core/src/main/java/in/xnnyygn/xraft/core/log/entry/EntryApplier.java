package in.xnnyygn.xraft.core.log.entry;

public interface EntryApplier {

    void applyEntry(Entry entry);

}
