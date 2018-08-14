package in.xnnyygn.xraft.core.log.snapshot;

public interface SnapshotGenerateStrategy {

    boolean shouldGenerate(int firstLogIndex, int lastApplied);

    SnapshotGenerateStrategy DISABLED = (firstLogIndex, lastApplied) -> false;

}
