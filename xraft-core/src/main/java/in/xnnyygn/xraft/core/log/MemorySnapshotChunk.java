package in.xnnyygn.xraft.core.log;

public class MemorySnapshotChunk implements SnapshotChunk {

    private final byte[] bytes;
    private final boolean lastChunk;

    public MemorySnapshotChunk(byte[] bytes, boolean lastChunk) {
        this.bytes = bytes;
        this.lastChunk = lastChunk;
    }

    @Override
    public boolean isLastChunk() {
        return this.lastChunk;
    }

    @Override
    public byte[] toByteArray() {
        return this.bytes;
    }

}
