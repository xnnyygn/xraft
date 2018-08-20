package in.xnnyygn.xraft.core.log.event;

public class SnapshotGenerateEvent {

    private final int lastIncludedIndex;

    public SnapshotGenerateEvent(int lastIncludedIndex) {
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public int getLastIncludedIndex() {
        return lastIncludedIndex;
    }

}
