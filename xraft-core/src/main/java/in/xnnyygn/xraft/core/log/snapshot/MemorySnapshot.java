package in.xnnyygn.xraft.core.log.snapshot;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class MemorySnapshot implements Snapshot {

    private final int lastIncludedIndex;
    private final int lastIncludedTerm;
    private final byte[] data;

    public MemorySnapshot(int lastIncludedIndex, int lastIncludedTerm) {
        this(lastIncludedIndex, lastIncludedTerm, new byte[0]);
    }

    public MemorySnapshot(int lastIncludedIndex, int lastIncludedTerm, byte[] data) {
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.data = data;
    }

    @Override
    public int getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    @Override
    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    @Override
    public long getDataSize() {
        return data.length;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public SnapshotChunk readData(int offset, int length) {
        if (offset < 0 || offset > data.length) {
            throw new IndexOutOfBoundsException("offset " + offset + " out of bound");
        }

        int bufferLength = Math.min(data.length - offset, length);
        byte[] buffer = new byte[bufferLength];
        System.arraycopy(data, offset, buffer, 0, bufferLength);
        return new SnapshotChunk(buffer, offset + length >= this.data.length);
    }

    @Override
    public InputStream getDataStream() {
        return new ByteArrayInputStream(data);
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return "MemorySnapshot{" +
                "lastIncludedIndex=" + lastIncludedIndex +
                ", lastIncludedTerm=" + lastIncludedTerm +
                ", data.size=" + data.length +
                '}';
    }

}
