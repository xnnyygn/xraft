package in.xnnyygn.xraft.core.log;

public interface TaskReference {

    enum Result {
        OK,
        TIMEOUT,
        UNKNOWN
    }

    Result getResult();

    boolean await(long timeout) throws InterruptedException;

}
