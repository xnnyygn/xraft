package in.xnnyygn.xraft.core.log.snapshot;

class DefaultSnapshotChunk implements SnapshotChunk {

    private final byte[] bytes;
    private final boolean lastChunk;

    DefaultSnapshotChunk(byte[] bytes, boolean lastChunk) {
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
