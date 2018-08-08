package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MemorySnapshotBuilder extends AbstractSnapshotBuilder {

    private final ByteArrayOutputStream output;

    public MemorySnapshotBuilder(InstallSnapshotRpc firstRpc) {
        super(firstRpc);
        output = new ByteArrayOutputStream();

        try {
            output.write(firstRpc.getData());
        } catch (IOException e) {
            throw new SnapshotIOException(e);
        }
    }

    @Override
    protected void doWrite(byte[] data) throws IOException {
        output.write(data);
    }

    @Override
    public Snapshot build() {
        return new MemorySnapshot(lastIncludedIndex, lastIncludedTerm, output.toByteArray());
    }

    @Override
    public void close() {
    }

}
