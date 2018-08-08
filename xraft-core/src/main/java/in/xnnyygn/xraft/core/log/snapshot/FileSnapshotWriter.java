package in.xnnyygn.xraft.core.log.snapshot;

import java.io.*;

public class FileSnapshotWriter implements AutoCloseable {

    private final DataOutputStream output;

    public FileSnapshotWriter(File file, int lastIncludedIndex, int lastIncludedTerm) throws IOException {
        this(new DataOutputStream(new FileOutputStream(file)), lastIncludedIndex, lastIncludedTerm);
    }

    FileSnapshotWriter(OutputStream output, int lastIncludedIndex, int lastIncludedTerm) throws IOException {
        this.output = new DataOutputStream(output);
        this.output.writeInt(lastIncludedIndex);
        this.output.writeInt(lastIncludedTerm);
    }

    public OutputStream getOutput() {
        return output;
    }

    public void write(byte[] data) throws IOException {
        output.write(data);
    }

    @Override
    public void close() throws IOException {
        output.close();
    }

}
