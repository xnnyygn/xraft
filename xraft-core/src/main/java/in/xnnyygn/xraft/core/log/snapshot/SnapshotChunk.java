package in.xnnyygn.xraft.core.log.snapshot;

public class SnapshotChunk {

    private final byte[] bytes;
    private final boolean lastChunk;

    SnapshotChunk(byte[] bytes, boolean lastChunk) {
        this.bytes = bytes;
        this.lastChunk = lastChunk;
    }

    public boolean isLastChunk() {
        return lastChunk;
    }

    public byte[] toByteArray() {
        return bytes;
    }

}
