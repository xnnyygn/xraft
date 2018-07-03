package in.xnnyygn.xraft.core.log;

public class NullSnapshotApplier implements SnapshotApplier {

    @Override
    public void applySnapshot(byte[] snapshot) {
    }

}
