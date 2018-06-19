package in.xnnyygn.xraft.core.service;

public class ChannelException extends RuntimeException {

    public ChannelException() {
    }

    public ChannelException(String message) {
        super(message);
    }

    public ChannelException(String message, Throwable cause) {
        super(message, cause);
    }

}
