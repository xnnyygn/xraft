package in.xnnyygn.xraft.core.log.snapshot;

public interface SnapshotChunk {

    boolean isLastChunk();

    byte[] toByteArray();

}
