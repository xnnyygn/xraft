package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MemorySnapshotBuilder implements SnapshotBuilder {

    private final ByteArrayOutputStream output;
    private final int lastIncludedIndex;
    private final int lastIncludedTerm;
    private int offset;

    public MemorySnapshotBuilder(InstallSnapshotRpc firstRpc) {
        assert firstRpc.getOffset() == 0;

        this.output = new ByteArrayOutputStream();
        this.lastIncludedIndex = firstRpc.getLastIncludedIndex();
        this.lastIncludedTerm = firstRpc.getLastIncludedTerm();

        try {
            this.output.write(firstRpc.getData());
        } catch (IOException e) {
            throw new SnapshotIOException("failed to write", e);
        }
        this.offset = firstRpc.getDataLength();
    }

    @Override
    public void append(InstallSnapshotRpc rpc) {
        if (rpc.getOffset() != offset) {
            throw new IllegalStateException("unexpected offset, expected " + offset + ", but was " + rpc.getOffset());
        }
        if (rpc.getLastIncludedIndex() != lastIncludedIndex || rpc.getLastIncludedTerm() != lastIncludedTerm) {
            throw new IllegalStateException("unexpected last included index or term");
        }
        try {
            output.write(rpc.getData());
        } catch (IOException e) {
            throw new SnapshotIOException("failed to write", e);
        }
        offset += rpc.getDataLength();
    }

    @Override
    public Snapshot build() {
        return new MemorySnapshot(lastIncludedIndex, lastIncludedTerm, output.toByteArray());
    }

}
