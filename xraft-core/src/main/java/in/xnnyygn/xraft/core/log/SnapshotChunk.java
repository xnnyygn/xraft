package in.xnnyygn.xraft.core.log;

public interface SnapshotChunk {

    boolean isLastChunk();

    byte[] toByteArray();

}
