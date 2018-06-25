package in.xnnyygn.xraft.core.log;

public class EntryApplierAdapter implements EntryApplier {

    private final CommandApplier callback;

    public EntryApplierAdapter(CommandApplier callback) {
        this.callback = callback;
    }

    @Override
    public void applyEntry(Entry entry) {
        this.callback.applyCommand(entry.getIndex(), entry.getCommand());
    }

}
