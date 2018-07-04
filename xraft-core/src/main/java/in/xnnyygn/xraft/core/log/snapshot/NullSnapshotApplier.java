package in.xnnyygn.xraft.core.log.snapshot;

public class NullSnapshotApplier implements SnapshotApplier {

    @Override
    public void applySnapshot(byte[] snapshot) {
    }

}
