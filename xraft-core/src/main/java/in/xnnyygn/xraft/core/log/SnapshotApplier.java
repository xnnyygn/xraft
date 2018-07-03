package in.xnnyygn.xraft.core.log;

public interface SnapshotApplier {

    void applySnapshot(byte[] snapshot);

}
