package in.xnnyygn.xraft.core.log.snapshot;

public class NullSnapshotGenerator implements SnapshotGenerator {

    @Override
    public byte[] generateSnapshot() {
        return new byte[0];
    }

}
