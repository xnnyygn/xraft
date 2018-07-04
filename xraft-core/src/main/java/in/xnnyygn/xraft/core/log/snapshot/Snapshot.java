package in.xnnyygn.xraft.core.log.snapshot;

public interface Snapshot {

    int getLastIncludedIndex();

    int getLastIncludedTerm();

    int size();

    SnapshotChunk read(int offset, int length);

    byte[] toByteArray();

    void close();

}
