package in.xnnyygn.xraft.core.log;

public interface EntryApplier {

    void applyEntry(Entry entry);

}
