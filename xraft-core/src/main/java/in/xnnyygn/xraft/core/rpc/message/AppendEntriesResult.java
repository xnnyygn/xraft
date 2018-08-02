package in.xnnyygn.xraft.core.rpc.message;

import java.io.Serializable;

public class AppendEntriesResult implements Serializable {

    private final String rpcMessageId;
    private final int term;
    private final boolean success;

    public AppendEntriesResult(String rpcMessageId, int term, boolean success) {
        this.rpcMessageId = rpcMessageId;
        this.term = term;
        this.success = success;
    }

    public String getRpcMessageId() {
        return rpcMessageId;
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
                "rpcMessageId='" + rpcMessageId + '\'' +
                ", success=" + success +
                ", term=" + term +
                '}';
    }

}
