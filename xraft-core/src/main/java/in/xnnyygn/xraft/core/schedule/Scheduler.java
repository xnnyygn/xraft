package in.xnnyygn.xraft.core.schedule;

// TODO add doc
public interface Scheduler {

    LogReplicationTask scheduleLogReplicationTask(Runnable task);

    ElectionTimeout scheduleElectionTimeout(Runnable task);

    void stop() throws InterruptedException;

}
