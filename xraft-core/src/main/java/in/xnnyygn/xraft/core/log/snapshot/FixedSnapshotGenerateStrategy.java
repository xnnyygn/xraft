package in.xnnyygn.xraft.core.log.snapshot;

public class FixedSnapshotGenerateStrategy implements SnapshotGenerateStrategy {

    private final int threshold;

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
