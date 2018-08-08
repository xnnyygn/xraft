package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.log.LogDir;
import in.xnnyygn.xraft.core.support.RandomAccessFileAdapter;
import in.xnnyygn.xraft.core.support.SeekableFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class FileSnapshot implements Snapshot {

    private static final int DATA_START = 4 + 4;
    private LogDir logDir;
    private SeekableFile seekableFile;
    private int lastIncludedIndex;
    private int lastIncludedTerm;
    private long dataLength;

    public FileSnapshot(LogDir logDir) {
        this.logDir = logDir;
        initialize(logDir.getSnapshotFile());
    }

    public FileSnapshot(File file) {
        initialize(file);
    }

    public FileSnapshot(SeekableFile seekableFile) {
        initialize(seekableFile);
    }

    private void initialize(File file) {
        try {
            initialize(new RandomAccessFileAdapter(file, "r"));
        } catch (IOException e) {
            throw new SnapshotIOException(e);
        }
    }

    private void initialize(SeekableFile seekableFile) {
        this.seekableFile = seekableFile;
        try {
            lastIncludedIndex = seekableFile.readInt();
            lastIncludedTerm = seekableFile.readInt();
            dataLength = seekableFile.size() - DATA_START;
        } catch (IOException e) {
            throw new SnapshotIOException(e);
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
    public long getDataSize() {
        return dataLength;
    }

    @Override
    public SnapshotChunk readData(int offset, int length) {
        if (offset > dataLength) {
            throw new IllegalArgumentException("offset > data length");
        }
        try {
            seekableFile.seek(DATA_START + offset);
            byte[] buffer = new byte[Math.min(length, (int) dataLength - offset)];
            int n = seekableFile.read(buffer);
            return new DefaultSnapshotChunk(buffer, offset + n >= dataLength);
        } catch (IOException e) {
            throw new SnapshotIOException("failed to seek or read file content", e);
        }
    }

    @Override
    public InputStream getDataStream() {
        try {
            return seekableFile.inputStream(DATA_START);
        } catch (IOException e) {
            throw new SnapshotIOException(e);
        }
    }

    public LogDir getLogDir() {
        return logDir;
    }

    @Override
    public void close() {
        try {
            seekableFile.close();
        } catch (IOException e) {
            throw new SnapshotIOException(e);
        }
    }

}
