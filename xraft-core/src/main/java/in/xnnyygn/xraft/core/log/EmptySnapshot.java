package in.xnnyygn.xraft.core.log;

public class EmptySnapshot implements Snapshot {

    @Override
    public int getLastIncludedIndex() {
        return 0;
    }

    @Override
    public int getLastIncludedTerm() {
        return 0;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public SnapshotChunk read(int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] toByteArray() {
        return new byte[0];
    }

    @Override
    public void close() {
    }

}
