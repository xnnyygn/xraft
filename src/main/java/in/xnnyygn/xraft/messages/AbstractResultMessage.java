package in.xnnyygn.xraft.messages;

import in.xnnyygn.xraft.server.ServerId;

public abstract class AbstractResultMessage<T> implements RaftMessage {

    private final T result;
    private ServerId senderServerId; // TODO rename to sourceServerId
    private ServerId destinationServerId;

    AbstractResultMessage(T result) {
        this.result = result;
    }

    public T getResult() {
        return result;
    }

    public ServerId getSenderServerId() {
        return senderServerId;
    }

    public void setSenderServerId(ServerId senderServerId) {
        this.senderServerId = senderServerId;
    }

    public ServerId getDestinationServerId() {
        return destinationServerId;
    }

    public boolean isDestinationNodeIdPresent() {
        return this.destinationServerId != null;
    }

    public void setDestinationServerId(ServerId destinationServerId) {
        this.destinationServerId = destinationServerId;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "destinationServerId=" + destinationServerId +
                ", result=" + result +
                ", senderServerId=" + senderServerId +
                '}';
    }

}
