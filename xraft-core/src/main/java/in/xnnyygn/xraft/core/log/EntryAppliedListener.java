package in.xnnyygn.xraft.core.log;

public interface EntryAppliedListener {

    void entryApplied(Entry entry);

}
