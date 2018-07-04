package in.xnnyygn.xraft.core.log.snapshot;

public interface SnapshotGenerator {

    // TODO replace byte[] with sth?
    byte[] generateSnapshot();

}
