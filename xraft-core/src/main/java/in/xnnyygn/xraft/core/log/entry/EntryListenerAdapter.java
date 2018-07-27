package in.xnnyygn.xraft.core.log.entry;

public abstract class EntryListenerAdapter implements EntryListener {

    @Override
    public void entryCommitted(Entry entry) {
    }

    @Override
    public void entryApplied(Entry entry) {
    }

}
