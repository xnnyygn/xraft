package in.xnnyygn.xraft.core.node;

import in.xnnyygn.xraft.core.log.CommandApplier;
import in.xnnyygn.xraft.core.log.EntryApplierAdapter;
import in.xnnyygn.xraft.core.log.SnapshotApplier;
import in.xnnyygn.xraft.core.log.SnapshotGenerator;
import in.xnnyygn.xraft.core.nodestate.NodeRole;
import in.xnnyygn.xraft.core.nodestate.NodeStateMachine;
import in.xnnyygn.xraft.core.nodestate.NodeStateSnapshot;
import in.xnnyygn.xraft.core.rpc.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node extends AbstractNode {

    private static final Logger logger = LoggerFactory.getLogger(Node.class);
    private final NodeContext context;
    private final NodeStateMachine stateMachine;

    public Node(NodeContext context, NodeStateMachine stateMachine, Endpoint endpoint) {
        super(context.getSelfNodeId(), endpoint);
        this.context = context;
        this.stateMachine = stateMachine;
    }

    public void start() {
        logger.info("Node {}, start", this.getId());
        this.context.initialize();
        this.stateMachine.start();
    }

    public NodeStateSnapshot getNodeState() {
        return this.stateMachine.getNodeState();
    }

    public void setCommandApplier(CommandApplier applier) {
        this.context.getLog().setEntryApplier(new EntryApplierAdapter(applier));
    }

    public void setSnapshotGenerator(SnapshotGenerator generator) {
        this.context.getLog().setSnapshotGenerator(generator);
    }

    public void setSnapshotApplier(SnapshotApplier applier) {
        this.context.getLog().setSnapshotApplier(applier);
    }

    public void appendLog(byte[] command, CommandApplier applier) {
        assert this.stateMachine.getNodeState().getRole() == NodeRole.LEADER;

        this.stateMachine.appendLog(command, new EntryApplierAdapter(applier));
    }

    public void stop() throws InterruptedException {
        logger.info("Node {}, stop", this.getId());
        this.stateMachine.stop();
        this.context.release();
    }

}
