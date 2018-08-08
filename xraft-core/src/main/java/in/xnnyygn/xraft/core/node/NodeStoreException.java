package in.xnnyygn.xraft.core.node;

public class NodeStoreException extends RuntimeException {

    public NodeStoreException(Throwable cause) {
        super(cause);
    }

    public NodeStoreException(String message, Throwable cause) {
        super(message, cause);
    }

}
