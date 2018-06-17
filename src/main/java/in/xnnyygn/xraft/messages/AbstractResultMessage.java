package in.xnnyygn.xraft.messages;

import in.xnnyygn.xraft.server.RaftNodeId;

public abstract class AbstractResultMessage<T> implements RaftMessage {

    private final T result;
    private RaftNodeId senderNodeId;
    private RaftNodeId destinationNodeId;

    AbstractResultMessage(T result) {
        this.result = result;
    }

    public T getResult() {
        return result;
    }

    public RaftNodeId getSenderNodeId() {
        return senderNodeId;
    }

    public void setSenderNodeId(RaftNodeId senderNodeId) {
        this.senderNodeId = senderNodeId;
    }

    public RaftNodeId getDestinationNodeId() {
        return destinationNodeId;
    }

    public boolean isDestinationNodeIdPresent() {
        return this.destinationNodeId != null;
    }

    public void setDestinationNodeId(RaftNodeId destinationNodeId) {
        this.destinationNodeId = destinationNodeId;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "destinationNodeId=" + destinationNodeId +
                ", result=" + result +
                ", senderNodeId=" + senderNodeId +
                '}';
    }

}
