package in.xnnyygn.xraft.core.log.entry;

public class EntryMeta {

    private final int kind;
    private final int index;
    private final int term;

    public EntryMeta(int kind, int index, int term) {
        this.kind = kind;
        this.index = index;
        this.term = term;
    }

    public int getKind() {
        return kind;
    }

    public int getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }

}
