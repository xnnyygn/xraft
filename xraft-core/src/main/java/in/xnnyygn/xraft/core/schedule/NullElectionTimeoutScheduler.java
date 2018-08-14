package in.xnnyygn.xraft.core.schedule;

public class NullElectionTimeoutScheduler implements ElectionTimeoutScheduler {

    public static final NullElectionTimeoutScheduler INSTANCE = new NullElectionTimeoutScheduler();

    @Override
    public ElectionTimeout scheduleElectionTimeout() {
        return new ElectionTimeout(new NullScheduledFuture(), this);
    }

}
