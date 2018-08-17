package in.xnnyygn.xraft.core.node.store;

/**
 * Thrown when failed to store state into store.
 */
public class NodeStoreException extends RuntimeException {

    /**
     * Create.
     *
     * @param cause cause
     */
    public NodeStoreException(Throwable cause) {
        super(cause);
    }

    /**
     * Create.
     *
     * @param message message
     * @param cause cause
     */
    public NodeStoreException(String message, Throwable cause) {
        super(message, cause);
    }

}
