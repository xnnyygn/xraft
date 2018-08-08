package in.xnnyygn.xraft.core.log.snapshot;

import java.io.InputStream;

public interface Snapshot {

    int getLastIncludedIndex();

    int getLastIncludedTerm();

    long getDataSize();

    SnapshotChunk readData(int offset, int length);

    InputStream getDataStream();

    void close();

}
