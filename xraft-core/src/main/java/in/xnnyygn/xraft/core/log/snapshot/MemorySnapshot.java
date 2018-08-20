package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import jdk.nashorn.internal.ir.annotations.Immutable;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;

@Immutable
public class MemorySnapshot implements Snapshot {

    private final int lastIncludedIndex;
    private final int lastIncludedTerm;
    private final byte[] data;
    private final Set<NodeEndpoint> lastConfig;

    public MemorySnapshot(int lastIncludedIndex, int lastIncludedTerm) {
        this(lastIncludedIndex, lastIncludedTerm, new byte[0], Collections.emptySet());
    }

    public MemorySnapshot(int lastIncludedIndex, int lastIncludedTerm, byte[] data, Set<NodeEndpoint> lastConfig) {
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.data = data;
        this.lastConfig = lastConfig;
    }

    @Override
    public int getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    @Override
    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    @Nonnull
    @Override
    public Set<NodeEndpoint> getLastConfig() {
        return lastConfig;
    }

    @Override
    public long getDataSize() {
        return data.length;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    @Nonnull
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
    @Nonnull
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
