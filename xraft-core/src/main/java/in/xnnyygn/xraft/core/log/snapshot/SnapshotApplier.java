package in.xnnyygn.xraft.core.log.snapshot;

public interface SnapshotApplier {

    void applySnapshot(byte[] snapshot);

}
