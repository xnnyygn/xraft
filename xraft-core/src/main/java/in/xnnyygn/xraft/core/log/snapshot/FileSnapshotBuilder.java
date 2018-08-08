package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.log.LogDir;
import in.xnnyygn.xraft.core.rpc.message.InstallSnapshotRpc;

import java.io.IOException;

public class FileSnapshotBuilder extends AbstractSnapshotBuilder {

    private final LogDir logDir;
    private FileSnapshotWriter writer;

    public FileSnapshotBuilder(InstallSnapshotRpc firstRpc, LogDir logDir) {
        super(firstRpc);
        this.logDir = logDir;

        try {
            writer = new FileSnapshotWriter(logDir.getSnapshotFile(), firstRpc.getLastIncludedIndex(), firstRpc.getLastIncludedTerm());
            writer.write(firstRpc.getData());
        } catch (IOException e) {
            throw new SnapshotIOException(e);
        }
    }

    @Override
    protected void doWrite(byte[] data) throws IOException {
        writer.write(data);
    }

    @Override
    public Snapshot build() {
        try {
            writer.close();
            return new FileSnapshot(logDir);
        } catch (IOException e) {
            throw new SnapshotIOException("failed to build", e);
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new SnapshotIOException(e);
        }
    }

}
