package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.log.LogException;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MemorySnapshotBuilder {

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
            throw new LogException(e);
        }
        this.offset = firstRpc.getDataLength();
    }

    public void append(InstallSnapshotRpc rpc) {
        if (rpc.getOffset() != this.offset) {
            throw new IllegalStateException("unexpected offset, expected " + this.offset + ", but was " + rpc.getOffset());
        }
        if (rpc.getLastIncludedIndex() != this.lastIncludedIndex || rpc.getLastIncludedTerm() != this.lastIncludedTerm) {
            throw new IllegalStateException("unexpected last included index or term");
        }
        try {
            this.output.write(rpc.getData());
        } catch (IOException e) {
            throw new LogException(e);
        }
        this.offset += rpc.getDataLength();
    }

    public MemorySnapshot build() {
        try {
            output.flush();
        } catch (IOException e) {
            throw new LogException(e);
        }
        return new MemorySnapshot(this.lastIncludedIndex, this.lastIncludedTerm, output.toByteArray());
    }

}
