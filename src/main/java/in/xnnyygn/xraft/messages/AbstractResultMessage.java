package in.xnnyygn.xraft.messages;

import in.xnnyygn.xraft.server.ServerId;

public abstract class AbstractResultMessage<T> implements RaftMessage {

    private final T result;
    private ServerId senderNodeId;
    private ServerId destinationNodeId;

    AbstractResultMessage(T result) {
        this.result = result;
    }

    public T getResult() {
        return result;
    }

    public ServerId getSenderNodeId() {
        return senderNodeId;
    }

    public void setSenderNodeId(ServerId senderNodeId) {
        this.senderNodeId = senderNodeId;
    }

    public ServerId getDestinationNodeId() {
        return destinationNodeId;
    }

    public boolean isDestinationNodeIdPresent() {
        return this.destinationNodeId != null;
    }

    public void setDestinationNodeId(ServerId destinationNodeId) {
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
