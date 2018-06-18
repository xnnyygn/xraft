package in.xnnyygn.xraft.rpc;

import in.xnnyygn.xraft.server.ServerId;
import in.xnnyygn.xraft.serverstate.ServerStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EmbeddedChannel implements Channel {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedChannel.class);
    private final ExecutorService executorService;
    private final ServerId selfServerId;
    private final ServerStateMachine serverStateMachine;

    public EmbeddedChannel(ServerId selfServerId, ServerStateMachine serverStateMachine) {
        this.executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "embedded-channel-" + selfServerId));
        this.selfServerId = selfServerId;
        this.serverStateMachine = serverStateMachine;
    }

    @Override
    public void write(Object payload, ServerId senderId) {
        this.executorService.submit(() -> this.dispatch(payload, senderId));
    }

    private void dispatch(Object payload, ServerId senderId) {
        if (payload instanceof RequestVoteRpc) {
            this.serverStateMachine.onReceiveRequestVoteRpc((RequestVoteRpc) payload);
        } else if (payload instanceof RequestVoteResult) {
            this.serverStateMachine.onReceiveRequestVoteResult((RequestVoteResult) payload, senderId);
        } else if (payload instanceof AppendEntriesRpc) {
            this.serverStateMachine.onReceiveAppendEntriesRpc((AppendEntriesRpc) payload);
        }
    }

    public void close() throws InterruptedException {
        logger.debug("Server {}, stop embedded channel", this.selfServerId);
        this.executorService.shutdown();
        this.executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

}
