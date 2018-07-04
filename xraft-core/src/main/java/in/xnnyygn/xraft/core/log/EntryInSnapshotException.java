package in.xnnyygn.xraft.core.log;

public class EntryInSnapshotException extends RuntimeException {

    private final int index;

    EntryInSnapshotException(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

}
