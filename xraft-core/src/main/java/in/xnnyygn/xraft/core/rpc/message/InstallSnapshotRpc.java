package in.xnnyygn.xraft.core.rpc.message;

import in.xnnyygn.xraft.core.node.NodeEndpoint;
import in.xnnyygn.xraft.core.node.NodeId;

import java.util.Set;

public class InstallSnapshotRpc {

    private int term;
    private NodeId leaderId;
    private int lastIndex;
    private int lastTerm;
    private Set<NodeEndpoint> lastConfig;
    private int offset;
    private byte[] data;
    private boolean done;

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public NodeId getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(NodeId leaderId) {
        this.leaderId = leaderId;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public void setLastIndex(int lastIndex) {
        this.lastIndex = lastIndex;
    }

    public int getLastTerm() {
        return lastTerm;
    }

    public void setLastTerm(int lastTerm) {
        this.lastTerm = lastTerm;
    }

    public Set<NodeEndpoint> getLastConfig() {
        return lastConfig;
    }

    public void setLastConfig(Set<NodeEndpoint> lastConfig) {
        this.lastConfig = lastConfig;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public byte[] getData() {
        return data;
    }

    public int getDataLength() {
        return this.data.length;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    @Override
    public String toString() {
        return "InstallSnapshotRpc{" +
                "data.size=" + (data != null ? data.length : 0) +
                ", done=" + done +
                ", lastIndex=" + lastIndex +
                ", lastTerm=" + lastTerm +
                ", leaderId=" + leaderId +
                ", offset=" + offset +
                ", term=" + term +
                '}';
    }

}
