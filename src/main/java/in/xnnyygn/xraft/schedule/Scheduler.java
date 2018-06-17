package in.xnnyygn.xraft.schedule;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import in.xnnyygn.xraft.messages.SimpleMessage;
import in.xnnyygn.xraft.server.ServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Scheduler implements ElectionTimeoutScheduler {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
    private final Random electionTimeoutRandom;
    private final ScheduledExecutorService scheduledExecutor;
    private final ActorSystem actorSystem;
    private final ServerId selfServerId;

    public Scheduler(ServerId selfServerId, ActorSystem actorSystem) {
        this.electionTimeoutRandom = new Random();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        this.selfServerId = selfServerId;
        this.actorSystem = actorSystem;
    }

    private ActorSelection getElectionActor() {
        return this.actorSystem.actorSelection("/user/election");
    }

    public LogReplicationTask scheduleLogReplicationTask() {
        logger.debug("Server {}, schedule log replication task", this.selfServerId);
        ScheduledFuture<?> scheduledFuture = scheduledExecutor.scheduleWithFixedDelay(
                () -> {
                    getElectionActor().tell(new SimpleMessage(SimpleMessage.Kind.LOG_REPLICATION), ActorRef.noSender());
                }, 0, 1000, TimeUnit.MILLISECONDS);
        return new LogReplicationTask(scheduledFuture, this.selfServerId);
    }

    // TODO use runner as parameter
    @Override
    public ElectionTimeout scheduleElectionTimeout() {
        logger.debug("Server {}, schedule election timeout", this.selfServerId);
        int timeout = electionTimeoutRandom.nextInt(2000) + 3000;
        ScheduledFuture<?> scheduledFuture = scheduledExecutor.schedule(
                () -> {
                    getElectionActor().tell(new SimpleMessage(SimpleMessage.Kind.ELECTION_TIMEOUT), ActorRef.noSender());
                }, timeout, TimeUnit.MILLISECONDS);
        return new ElectionTimeout(scheduledFuture, this, this.selfServerId);
    }

    public void stop() throws InterruptedException {
        this.scheduledExecutor.shutdown();
        this.scheduledExecutor.awaitTermination(1, TimeUnit.SECONDS);
    }

}
