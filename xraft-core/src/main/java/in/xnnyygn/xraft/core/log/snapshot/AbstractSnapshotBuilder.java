package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

import java.io.IOException;

abstract class AbstractSnapshotBuilder implements SnapshotBuilder {

    protected int lastIncludedIndex;
    protected int lastIncludedTerm;
    private int offset;

    public AbstractSnapshotBuilder(InstallSnapshotRpc firstRpc) {
        assert firstRpc.getOffset() == 0;

        lastIncludedIndex = firstRpc.getLastIncludedIndex();
        lastIncludedTerm = firstRpc.getLastIncludedTerm();
        offset = firstRpc.getDataLength();
    }

    protected void write(byte[] data) {
        try {
            doWrite(data);
        } catch (IOException e) {
            throw new SnapshotIOException(e);
        }
    }

    protected abstract void doWrite(byte[] data) throws IOException;

    @Override
    public void append(InstallSnapshotRpc rpc) {
        if (rpc.getOffset() != offset) {
            throw new IllegalStateException("unexpected offset, expected " + offset + ", but was " + rpc.getOffset());
        }
        if (rpc.getLastIncludedIndex() != lastIncludedIndex || rpc.getLastIncludedTerm() != lastIncludedTerm) {
            throw new IllegalStateException("unexpected last included index or term");
        }
        write(rpc.getData());
        offset += rpc.getDataLength();
    }

}
