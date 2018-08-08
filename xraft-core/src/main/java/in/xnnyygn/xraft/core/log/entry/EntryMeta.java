package in.xnnyygn.xraft.core.log.entry;

public class EntryMeta {

    private final int index;
    private final int term;

    public EntryMeta(int index, int term) {
        this.index = index;
        this.term = term;
    }

    public int getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }

}
