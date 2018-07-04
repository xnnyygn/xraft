package in.xnnyygn.xraft.core.log.entry;

public abstract class AbstractEntry implements Entry {

    protected final int index;
    protected final int term;

    AbstractEntry(int index, int term) {
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
