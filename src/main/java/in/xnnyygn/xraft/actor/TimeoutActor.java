package in.xnnyygn.xraft.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;
import in.xnnyygn.xraft.server.ServerId;
import in.xnnyygn.xraft.messages.SimpleMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TimeoutActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(TimeoutActor.class);
    private final ScheduledExecutorService scheduler;
    private final ServerId selfServerId;
    private final Random electionTimeoutRandom;
    private ScheduledFuture<?> electionTimeoutFuture;

    public TimeoutActor(ServerId selfServerId) {
        super();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.selfServerId = selfServerId;
        this.electionTimeoutRandom = new Random();
    }

    @Override
    public void postStop() throws Exception {
        this.scheduler.shutdown();
        this.scheduler.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(SimpleMessage.class, msg -> {
            switch (msg.getKind()) {
                case ELECTION_TIMEOUT_REGISTRATION:
                    registerElectionTimeout();
                    break;
                case ELECTION_TIMEOUT_DEREGISTRATION:
                    deregisterElectionTimeout();
                    break;
                case ELECTION_TIMEOUT_RESET:
                    resetElectionTimeout();
                    break;
                case TEST:
                    testActor();
                    break;
            }
        }).build();
    }

    private ActorSelection getElectionActor() {
        return getContext().actorSelection(RaftActorPaths.ACTOR_PATH_ELECTION);
    }

    private void testActor() {
        System.out.println("receive test message from " + getSender().path());
    }

    private void registerElectionTimeout() {
        int timeout = electionTimeoutRandom.nextInt(2000) + 1000;
        logger.debug("Node {}, register election timeout, {}ms", this.selfServerId, timeout);
        // TODO check if registered

        this.electionTimeoutFuture = this.scheduler.schedule(() -> {
            getElectionActor().tell(new SimpleMessage(SimpleMessage.Kind.ELECTION_TIMEOUT), getSelf());
        }, timeout, TimeUnit.MILLISECONDS);
    }

    private void deregisterElectionTimeout() {
        if (this.electionTimeoutFuture != null) {
            logger.debug("Node {}, deregister election timeout", this.selfServerId);
            this.electionTimeoutFuture.cancel(false);
            this.electionTimeoutFuture = null;
        }
    }

    private void resetElectionTimeout() {
        deregisterElectionTimeout();
        registerElectionTimeout();
    }
}
