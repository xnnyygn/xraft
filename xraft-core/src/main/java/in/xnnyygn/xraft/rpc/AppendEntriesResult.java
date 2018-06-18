package in.xnnyygn.xraft.rpc;

import java.io.Serializable;

public class AppendEntriesResult implements Serializable {

    private final int term;
    private final boolean success;

    public AppendEntriesResult(int term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "AppendEntriesResult{" +
                "success=" + success +
                ", term=" + term +
                '}';
    }

}
