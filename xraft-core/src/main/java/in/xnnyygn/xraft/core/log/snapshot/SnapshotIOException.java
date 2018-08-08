package in.xnnyygn.xraft.core.log.snapshot;

import in.xnnyygn.xraft.core.log.LogException;

public class SnapshotIOException extends LogException {

    public SnapshotIOException(String message) {
        super(message);
    }

    public SnapshotIOException(Throwable cause) {
        super(cause);
    }

    public SnapshotIOException(String message, Throwable cause) {
        super(message, cause);
    }

}
