package in.xnnyygn.xraft.core.log;

public class NullSnapshotGenerator implements SnapshotGenerator {

    @Override
    public byte[] generateSnapshot() {
        return new byte[0];
    }

}
