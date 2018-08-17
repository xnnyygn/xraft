package in.xnnyygn.xraft.core.log.snapshot;

/**
 * Threshold based strategy.
 * <p>
 * If last applied log index - first log index in file &gt; threshold, generate snapshot.
 * </p>
 */
public class FixedSnapshotGenerateStrategy implements SnapshotGenerateStrategy {

    private final int threshold;

    /**
     * Create.
     *
     * @param threshold threshold
     * @throws IllegalArgumentException if threshold is negative
     */
    public FixedSnapshotGenerateStrategy(int threshold) {
        if (threshold < 0) {
            throw new IllegalArgumentException("threshold < 0");
        }
        this.threshold = threshold;
    }

    @Override
    public boolean shouldGenerate(int firstLogIndex, int lastApplied) {
        return (lastApplied - firstLogIndex >= threshold);
    }

}
