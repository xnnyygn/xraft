package in.xnnyygn.xraft.core.rpc;

import in.xnnyygn.xraft.core.node.NodeId;
import in.xnnyygn.xraft.core.nodestate.NodeStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EmbeddedChannel implements Channel {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedChannel.class);
    private final ExecutorService executorService;
    private final NodeStateMachine nodeStateMachine;

    public EmbeddedChannel(NodeId selfNodeId, NodeStateMachine nodeStateMachine) {
        this.executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "embedded-channel-" + selfNodeId));
        this.nodeStateMachine = nodeStateMachine;
    }

    @Override
    public void send(Object payload, NodeId senderId) {
        this.executorService.submit(() -> this.dispatch(payload, senderId));
    }

    private void dispatch(Object payload, NodeId senderId) {
        if (payload instanceof RequestVoteRpc) {
            this.nodeStateMachine.onReceiveRequestVoteRpc((RequestVoteRpc) payload);
        } else if (payload instanceof RequestVoteResult) {
            this.nodeStateMachine.onReceiveRequestVoteResult((RequestVoteResult) payload, senderId);
        } else if (payload instanceof AppendEntriesRpc) {
            this.nodeStateMachine.onReceiveAppendEntriesRpc((AppendEntriesRpc) payload);
        }
    }

    public void close() {
        logger.debug("stop embedded channel");
        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new ChannelException("failed to stop channel", e);
        }
    }

}
