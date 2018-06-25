package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.CommandApplyAdapter;
import in.xnnyygn.xraft.core.log.CommandApplyListener;
import in.xnnyygn.xraft.core.log.EntryAppliedListener;
import in.xnnyygn.xraft.core.nodestate.NodeRole;
import in.xnnyygn.xraft.core.nodestate.NodeStateMachine;
import in.xnnyygn.xraft.core.nodestate.NodeStateSnapshot;
import in.xnnyygn.xraft.core.rpc.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node extends AbstractNode {

    private static final Logger logger = LoggerFactory.getLogger(Node.class);
    private final NodeContext context;
    private final NodeStateMachine stateMachine;
    private final Channel channel;
    private CommandApplyAdapter commandApplyAdapter;

    public Node(NodeContext context, NodeStateMachine stateMachine, Channel channel) {
        super(context.getSelfNodeId());
        this.context = context;
        this.stateMachine = stateMachine;
        this.channel = channel;
    }

    public void start() {
        logger.info("Node {}, start", this.getId());
        this.stateMachine.start();
    }

    public Channel getChannel() {
        return this.channel;
    }

    public NodeStateSnapshot getNodeState() {
        return this.stateMachine.getNodeState();
    }

    public void appendLog(byte[] command, EntryAppliedListener listener) {
        assert this.stateMachine.getNodeState().getRole() == NodeRole.LEADER;

        this.stateMachine.appendLog(command, listener);
    }

    public void setCommandApplyListener(CommandApplyListener listener) {
        this.commandApplyAdapter = new CommandApplyAdapter(listener);
        this.context.register(this.commandApplyAdapter);
    }

    public void stop() throws InterruptedException {
        logger.info("Node {}, stop", this.getId());
        this.stateMachine.stop();
        this.context.release();
    }

}
