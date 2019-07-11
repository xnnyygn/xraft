package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.Protos;
import in.xnnyygn.xraft.core.node.NodeEndpoint;

import java.io.*;
import java.util.Set;
import java.util.stream.Collectors;

public class FileSnapshotWriter implements SnapshotWriter {

    private final DataOutputStream output;

    public FileSnapshotWriter(File file, int lastIncludedIndex, int lastIncludedTerm, Set<NodeEndpoint> lastConfig) throws IOException {
        this(new DataOutputStream(new FileOutputStream(file)), lastIncludedIndex, lastIncludedTerm, lastConfig);
    }

    FileSnapshotWriter(OutputStream output, int lastIncludedIndex, int lastIncludedTerm, Set<NodeEndpoint> lastConfig) throws IOException {
        this.output = new DataOutputStream(output);
        byte[] headerBytes = Protos.SnapshotHeader.newBuilder()
                .setLastIndex(lastIncludedIndex)
                .setLastTerm(lastIncludedTerm)
                .addAllLastConfig(
                        lastConfig.stream()
                                .map(e -> Protos.NodeEndpoint.newBuilder()
                                        .setId(e.getId().getValue())
                                        .setHost(e.getHost())
                                        .setPort(e.getPort())
                                        .build())
                                .collect(Collectors.toList()))
                .build().toByteArray();
        this.output.writeInt(headerBytes.length);
        this.output.write(headerBytes);

    }

    @Override
    public OutputStream getOutput() {
        return output;
    }

    @Override
    public void write(byte[] data) throws IOException {
        output.write(data);
    }

    @Override
    public void close() throws IOException {
        output.close();
    }

}
