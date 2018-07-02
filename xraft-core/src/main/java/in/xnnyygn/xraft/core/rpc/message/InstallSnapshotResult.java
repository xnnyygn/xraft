package in.xnnyygn.xraft.core.rpc.message;

public class InstallSnapshotResult {

    private final int term;

    public InstallSnapshotResult(int term) {
        this.term = term;
    }

    public int getTerm() {
        return term;
    }

    @Override
    public String toString() {
        return "InstallSnapshotResult{" +
                "term=" + term +
                '}';
    }

}
