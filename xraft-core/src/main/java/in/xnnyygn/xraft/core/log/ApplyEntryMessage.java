package in.xnnyygn.xraft.core.log;

public class ApplyEntryMessage {

    private final Entry entry;

    public ApplyEntryMessage(Entry entry) {
        this.entry = entry;
    }

    public Entry getEntry() {
        return entry;
    }

    @Override
    public String toString() {
        return "ApplyEntryMessage{" + entry + '}';
    }

}
