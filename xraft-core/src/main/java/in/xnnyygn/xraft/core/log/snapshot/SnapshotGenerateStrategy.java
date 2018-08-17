package in.xnnyygn.xraft.core.log.snapshot;

/**
 * Strategy of generating snapshot.
 */
public interface SnapshotGenerateStrategy {

    /**
     * Should generate or not.
     *
     * @param firstLogIndex first log index in log files, may not be {@code 0}
     * @param lastApplied last applied log index
     * @return true if should generate, otherwise false
     */
    boolean shouldGenerate(int firstLogIndex, int lastApplied);

    /**
     * Disable snapshot.
     */
    SnapshotGenerateStrategy DISABLED = (firstLogIndex, lastApplied) -> false;

}
