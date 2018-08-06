package in.xnnyygn.xraft.core.log.snapshot;

import java.io.*;

public class SnapshotFile implements Snapshot {

    private static final int DATA_START = 4 + 4 + 4;
    private final String filename;
    private RandomAccessFile file;
    private int lastIncludedIndex;
    private int lastIncludedTerm;
    private int dataLength;

    public SnapshotFile(String filename) {
        this.filename = filename;
        try {
            this.file = new RandomAccessFile(filename, "r");
        } catch (FileNotFoundException e) {
            throw new SnapshotIOException("failed to open", e);
        }
        readMeta();
    }

    private void readMeta() {
        try {
            lastIncludedIndex = file.readInt();
            lastIncludedTerm = file.readInt();
            dataLength = file.readInt();
        } catch (IOException e) {
            throw new SnapshotIOException("failed to read", e);
        }
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
    public int size() {
        return dataLength;
    }

    @Override
    public SnapshotChunk read(int offset, int length) {
        if (offset > dataLength) {
            throw new IllegalArgumentException("offset > data length");
        }
        try {
            file.seek(DATA_START + offset);
            byte[] buffer = new byte[length];
            file.read(buffer);
            return new DefaultSnapshotChunk(buffer, offset + length >= dataLength);
        } catch (IOException e) {
            throw new SnapshotIOException("failed to seek or read file content", e);
        }
    }

    @Override
    public byte[] toByteArray() {
        byte[] buffer = new byte[dataLength];
        try {
            file.seek(DATA_START);
            file.read(buffer);
        } catch (IOException e) {
            throw new SnapshotIOException("failed to seek or read file content", e);
        }
        return buffer;
    }

    public void replace(Snapshot snapshot) {
        close();
        try {
            write(snapshot, filename);
            file = new RandomAccessFile(filename, "r");
        } catch (IOException e) {
            throw new SnapshotIOException("failed to read or write", e);
        }
        readMeta();
    }

    public static SnapshotFile create(Snapshot snapshot, String filename) {
        try {
            write(snapshot, filename);
            return new SnapshotFile(filename);
        } catch (IOException e) {
            throw new SnapshotIOException("failed to write", e);
        }
    }

    private static void write(Snapshot snapshot, String filename) throws IOException {
        try (DataOutputStream output = new DataOutputStream(new FileOutputStream(filename))) {
            output.writeInt(snapshot.getLastIncludedIndex());
            output.writeInt(snapshot.getLastIncludedTerm());
            output.writeInt(snapshot.size());
            output.write(snapshot.toByteArray());
        }
    }

    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            throw new SnapshotIOException("failed to close file", e);
        }
    }

}
