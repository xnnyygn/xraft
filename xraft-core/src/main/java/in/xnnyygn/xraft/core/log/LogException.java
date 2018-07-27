package in.xnnyygn.xraft.core.log;

public class LogException extends RuntimeException {

    public LogException() {
    }

    public LogException(Throwable cause) {
        super(cause);
    }

    public LogException(String message, Throwable cause) {
        super(message, cause);
    }

}
