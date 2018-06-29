package in.xnnyygn.xraft.kvstore.message;

public class Failure {

    private final int errorCode;
    private final String message;

    public Failure(int errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "Failure{" +
                "errorCode=" + errorCode +
                ", message='" + message + '\'' +
                '}';
    }

}
