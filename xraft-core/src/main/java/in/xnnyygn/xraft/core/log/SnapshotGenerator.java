package in.xnnyygn.xraft.core.log;

public interface SnapshotGenerator {

    // TODO replace byte[] with sth?
    byte[] generateSnapshot();

}
