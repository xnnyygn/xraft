package in.xnnyygn.xraft.core.schedule;

public class NullElectionTimeoutScheduler implements ElectionTimeoutScheduler {

    @Override
    public ElectionTimeout scheduleElectionTimeout() {
        return new ElectionTimeout(new NullScheduledFuture(), this);
    }

}
