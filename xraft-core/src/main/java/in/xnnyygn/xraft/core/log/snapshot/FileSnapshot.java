package in.xnnyygn.xraft.core.log.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import in.xnnyygn.xraft.core.Protos;
import in.xnnyygn.xraft.core.log.LogDir;
import in.xnnyygn.xraft.core.log.LogException;
import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.support.RandomAccessFileAdapter;
import in.xnnyygn.xraft.core.support.SeekableFile;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;

public class FileSnapshot implements Snapshot {

    private LogDir logDir;
    private SeekableFile seekableFile;
    private int lastIncludedIndex;
    private int lastIncludedTerm;
    private Set<NodeEndpoint> lastConfig;
    private long dataStart;
    private long dataLength;

    public FileSnapshot(LogDir logDir) {
        this.logDir = logDir;
        readHeader(logDir.getSnapshotFile());
    }

    public FileSnapshot(File file) {
        readHeader(file);
    }

    public FileSnapshot(SeekableFile seekableFile) {
        readHeader(seekableFile);
    }

    private void readHeader(File file) {
        try {
            readHeader(new RandomAccessFileAdapter(file, "r"));
        } catch (FileNotFoundException e) {
            throw new LogException(e);
        }
    }

    private void readHeader(SeekableFile seekableFile) {
        this.seekableFile = seekableFile;
        try {
            int headerLength = seekableFile.readInt();
            byte[] headerBytes = new byte[headerLength];
            seekableFile.read(headerBytes);
            Protos.SnapshotHeader header = Protos.SnapshotHeader.parseFrom(headerBytes);
            lastIncludedIndex = header.getLastIndex();
            lastIncludedTerm = header.getLastTerm();
            lastConfig = header.getLastConfigList().stream()
                    .map(e -> new NodeEndpoint(e.getId(), e.getHost(), e.getPort()))
                    .collect(Collectors.toSet());
            dataStart = seekableFile.position();
            dataLength = seekableFile.size() - dataStart;
        } catch (InvalidProtocolBufferException e) {
            throw new LogException("failed to parse header of snapshot", e);
        } catch (IOException e) {
            throw new LogException("failed to read snapshot", e);
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

    @Nonnull
    @Override
    public Set<NodeEndpoint> getLastConfig() {
        return lastConfig;
    }

    @Override
    public long getDataSize() {
        return dataLength;
    }

    @Override
    @Nonnull
    public SnapshotChunk readData(int offset, int length) {
        if (offset > dataLength) {
            throw new IllegalArgumentException("offset > data length");
        }
        try {
            seekableFile.seek(dataStart + offset);
            byte[] buffer = new byte[Math.min(length, (int) dataLength - offset)];
            int n = seekableFile.read(buffer);
            return new SnapshotChunk(buffer, offset + n >= dataLength);
        } catch (IOException e) {
            throw new LogException("failed to seek or read snapshot content", e);
        }
    }

    @Override
    @Nonnull
    public InputStream getDataStream() {
        try {
            return seekableFile.inputStream(dataStart);
        } catch (IOException e) {
            throw new LogException("failed to get input stream of snapshot data", e);
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
            throw new LogException("failed to close file", e);
        }
    }

}
