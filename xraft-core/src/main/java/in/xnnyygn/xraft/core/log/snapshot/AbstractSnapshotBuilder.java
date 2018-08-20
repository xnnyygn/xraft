package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.log.LogException;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

import java.io.IOException;
import java.util.Set;

abstract class AbstractSnapshotBuilder<T extends Snapshot> implements SnapshotBuilder<T> {

    int lastIncludedIndex;
    int lastIncludedTerm;
    Set<NodeEndpoint> lastConfig;
    private int offset;

    AbstractSnapshotBuilder(InstallSnapshotRpc firstRpc) {
        assert firstRpc.getOffset() == 0;
        lastIncludedIndex = firstRpc.getLastIndex();
        lastIncludedTerm = firstRpc.getLastTerm();
        lastConfig = firstRpc.getLastConfig();
        offset = firstRpc.getDataLength();
    }

    protected void write(byte[] data) {
        try {
            doWrite(data);
        } catch (IOException e) {
            throw new LogException(e);
        }
    }

    protected abstract void doWrite(byte[] data) throws IOException;

    @Override
    public void append(InstallSnapshotRpc rpc) {
        if (rpc.getOffset() != offset) {
            throw new IllegalArgumentException("unexpected offset, expected " + offset + ", but was " + rpc.getOffset());
        }
        if (rpc.getLastIndex() != lastIncludedIndex || rpc.getLastTerm() != lastIncludedTerm) {
            throw new IllegalArgumentException("unexpected last included index or term");
        }
        write(rpc.getData());
        offset += rpc.getDataLength();
    }

}
