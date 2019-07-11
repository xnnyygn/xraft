package in.xnnyygn.xraft.core.log.snapshot;

import java.io.IOException;
import java.io.OutputStream;

public interface SnapshotWriter extends AutoCloseable {
    OutputStream getOutput();

    void write(byte[] data) throws IOException;
}
