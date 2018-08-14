package in.xnnyygn.xraft.core.log.snapshot;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

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
    public long getDataSize() {
        return 0;
    }

    @Override
    public SnapshotChunk readData(int offset, int length) {
        if (offset == 0) {
            return new SnapshotChunk(new byte[0], true);
        }
        throw new IllegalArgumentException("offset > 0");
    }

    @Override
    public InputStream getDataStream() {
        return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public void close() {
    }

}
