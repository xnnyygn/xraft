package in.xnnyygn.xraft.core.log.event;

public class SnapshotGeneratedEvent {

    private final int lastIncludedIndex;

    public SnapshotGeneratedEvent(int lastIncludedIndex) {
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public int getLastIncludedIndex() {
        return lastIncludedIndex;
    }

}
