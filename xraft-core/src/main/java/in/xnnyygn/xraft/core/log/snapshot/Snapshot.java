package in.xnnyygn.xraft.core.log.snapshot;

// struct?
public interface Snapshot {

    int getLastIncludedIndex();

    int getLastIncludedTerm();

    int size();

    // InstallSnapshot rpc
    SnapshotChunk read(int offset, int length);

    // read all data and apply to service when startup
    byte[] toByteArray();

}
